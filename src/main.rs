use std::cmp::min;
use std::fmt::Debug;
use std::fs::File;
use std::future::Future;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::time::Duration;
use std::{
    collections::{BTreeMap, BTreeSet},
    convert::TryFrom,
};

use anyhow::{anyhow, Result};
use chrono::{DateTime, Utc};
use clap::{App, Arg};
use http::header::{HeaderMap, HeaderName, HeaderValue};
use s3::bucket::Bucket;
use s3::creds::Credentials;
use s3::region::Region;
use sha2::{Digest, Sha512};
use tokio::fs::read_dir;
use tokio::task::spawn_blocking;
use tokio::time::timeout;

mod select;
use select::{select_vec, select_vec_try, selector, Selector};
mod tree;
use tree::{Difference, DifferenceValue, Tree};

#[derive(PartialEq, Debug)]
struct Config {
    bucket: Bucket,
    folder_base: PathBuf,
}

/// An abstraction for files
#[derive(Debug, Clone, PartialEq, Eq, Ord, PartialOrd)]
struct Object {
    last_modification_date: DateTime<Utc>,
    size: u64,
}

impl DifferenceValue for Object {
    type Item = Self;

    fn difference(&self, other: &Self) -> Option<Difference<Self::Item>> {
        if self.last_modification_date > other.last_modification_date {
            Some(Difference::Newer(self.clone()))
        } else {
            Some(Difference::Older(other.clone()))
        }
    }
}

async fn delete_object_in_bucket(bucket: &Bucket, path: &str) -> Result<()> {
    println!("Deleting {:?}", path);
    bucket.delete_object(path).await.map(|_| ())
}

async fn upload_object_to_bucket(
    bucket: &Bucket,
    sha512: &HeaderValue,
    local_path: &str,
    remote_path: &str,
) -> Result<()> {
    println!("Uploading {:?}", remote_path);
    let mut custom_headers = HeaderMap::new();
    custom_headers.insert(HeaderName::from_static("x-amz-meta-sha512"), sha512.clone());
    let status_code = bucket
        .put_object_stream_with_headers(local_path, remote_path, Some(custom_headers))
        .await?;
    assert_eq!(status_code, 200);
    Ok(())
}

/// Iterate over all the objects in the bucket, and generate a tree of their hierarchy
async fn read_elements_from_bucket(bucket: &Bucket) -> Result<Tree<String, Object>> {
    let mut res = Tree::new();

    let bucket_res_list = bucket.list("".into(), None).await?;

    for obj_list in bucket_res_list {
        let mut keys = Vec::with_capacity(obj_list.contents.len());
        for e in &obj_list.contents {
            let paths = e.key.split("/").map(|e| e.into()).collect();
            //let (object, _) = bucket.head_object(&e.key).await?;
            //let sha256 = if let Some(ref metadata) = object.metadata {
            //    metadata
            //        .get("sha256")
            //        .map(|x| x.as_str())
            //        .unwrap_or("")
            //        .to_string()
            //} else {
            //    String::from("")
            //};

            keys.push((
                paths,
                Object {
                    last_modification_date: DateTime::parse_from_rfc3339(&e.last_modified)
                        .unwrap()
                        .into(),
                    size: e.size,
                },
            ));
        }

        res.add_values_with_path(keys);
    }

    Ok(res)
}

/// read a file and compute its hash
fn hash_file(path: impl AsRef<Path>) -> Result<String, std::io::Error> {
    let mut fd = File::open(path)?;
    let mut digest = Sha512::new();
    let mut buf = vec![0; 4096];
    loop {
        let read = fd.read(&mut buf)?;
        if read == 0 {
            return Ok(format!("{:x}", digest.finalize()));
        }

        // we assume the sha512 operation to be fast enought on 4K blocks for its impact to be
        // reasonable on the async scheduler (for reference, openssl seems to be taking 8.2
        // microseconds to perform a sha512 on a 4k block on my laptop - Intel Coffee Lake.
        // Considering that number and the fact that we are only dealing with file I/Os, currently
        // blocking in all rust iasync runtimes until io_uring gets properly supported, I guess
        // that take is a fairly safe bet)
        digest.update(&buf[0..read]);
    }
}

impl Config {
    fn update_bucket<'a>(
        &'a self,
        bucket_object: &'a Tree<String, Object>,
        relative_path: Vec<String>,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + 'a>> {
        Box::pin(async move {
            let mut children = Vec::new();
            let mut objects = BTreeMap::new();

            let mut current_dir = self.folder_base.clone();
            for entry in relative_path.iter() {
                current_dir.push(entry);
            }

            let mut folder = read_dir(&current_dir).await?;
            while let Some(entry) = folder.next_entry().await? {
                let file_type = entry.file_type().await?;

                // TODO; symlinks !!!
                if file_type.is_dir() {
                    children.push(entry.file_name().to_string_lossy().to_string());
                } else if file_type.is_file() {
                    let metadata = entry.metadata().await?;
                    let modif_date = metadata
                        .modified()
                        .expect("Couldn't retrieve the file last modification time")
                        .into();
                    objects.insert(
                        entry.file_name().to_string_lossy().to_string(),
                        Object {
                            last_modification_date: modif_date,
                            size: metadata.len(),
                        },
                    );
                }
            }

            let mut to_delete = BTreeSet::new();
            let mut to_add = BTreeMap::new();

            if let Some(s3_objects) = bucket_object.get(relative_path.iter()) {
                let s3_objects: Vec<(&String, &Object)> = s3_objects.collect();
                for (file_path, object) in &objects {
                    if let Some((_, s3_obj)) =
                        s3_objects.iter().find(|(s3_path, _)| *s3_path == file_path)
                    {
                        if object.last_modification_date > s3_obj.last_modification_date {
                            to_add.insert(file_path, object.size);
                        }
                    } else {
                        to_add.insert(file_path, object.size);
                    }
                }
                for (s3_path, _) in s3_objects {
                    if objects.iter().find(|(path, _)| *path == s3_path).is_some() {
                        continue;
                    }
                    to_delete.insert(s3_path);
                }
            }

            let mut selector = Vec::new();
            let mut errors = Vec::new();

            for old_path in to_delete {
                let remote_path = {
                    let mut path = PathBuf::new();
                    for entry in relative_path.iter() {
                        path.push(entry);
                    }
                    path.push(old_path);
                    path.to_string_lossy().to_string()
                };
                // delete the file in the bucket
                selector.push(Box::pin(async move {
                    let nb_retry: usize = 3;
                    for _ in 0..nb_retry {
                        if let Ok(x) = timeout(
                            Duration::new(5, 0),
                            delete_object_in_bucket(&self.bucket, &remote_path),
                        )
                        .await
                        {
                            return x;
                        }
                    }
                    Err(anyhow!(
                        "An operation timed out {} times, aborting",
                        nb_retry
                    ))
                }));
            }

            select_vec_try(&mut selector, &mut errors, 15).await?;

            let mut selector = Vec::new();
            let mut errors = Vec::new();

            for (old_path, size) in to_add {
                let (path, remote_path) = {
                    let mut path = current_dir.clone();
                    path.push(old_path);
                    (
                        path.to_string_lossy().to_string(),
                        path.as_path()
                            .strip_prefix(&self.folder_base)?
                            .to_string_lossy()
                            .to_string(),
                    )
                };
                // delete the file in the bucket
                selector.push(Box::pin(async move {
                    let path_clone = path.clone();
                    let sha512 = HeaderValue::from_bytes(
                        spawn_blocking(move || hash_file(&path_clone))
                            .await??
                            .as_bytes(),
                    )?;
                    let nb_retry: usize = 3;
                    for _ in 0..nb_retry {
                        if let Ok(x) = timeout(
                            min(
                                Duration::new(86400, 0),
                                Duration::new(5 + size / 100_000, 0),
                            ),
                            upload_object_to_bucket(&self.bucket, &sha512, &path, &remote_path),
                        )
                        .await
                        {
                            return x;
                        }
                    }
                    Err(anyhow!(
                        "An operation timed out {} times, aborting",
                        nb_retry
                    ))
                }));
            }

            select_vec_try(&mut selector, &mut errors, 5).await?;

            for child in children {
                let mut relative_path = relative_path.clone();
                relative_path.push(child);

                self.update_bucket(bucket_object, relative_path).await?;
            }

            Ok(())
        })
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let matches = App::new("s3s")
        .version("0.1.8")
        .author("Simon Thoby <git@nightmared.fr>")
        .about("Sync a folder to a s3 bucket")
        .arg(
            Arg::with_name("Secret key")
                .short("s")
                .long("secret-key")
                .takes_value(true)
                .required(true),
        )
        .arg(
            Arg::with_name("Access key")
                .short("a")
                .long("access-key")
                .takes_value(true)
                .required(true),
        )
        .arg(
            Arg::with_name("Region")
                .long("region")
                .takes_value(true)
                .default_value("fr-par"),
        )
        .arg(
            Arg::with_name("Endpoint")
                .long("endpoint")
                .takes_value(true)
                .default_value("https://s3.fr-par.scw.cloud"),
        )
        .arg(
            Arg::with_name("Bucket")
                .short("b")
                .long("bucket")
                .takes_value(true)
                .required(true),
        )
        .arg(
            Arg::with_name("Folder")
                .short("f")
                .long("folder")
                .takes_value(true)
                .required(true),
        )
        .arg(
            Arg::with_name("Storage Class")
                .long("storage-class")
                .possible_values(&["standard", "glacier"])
                .takes_value(true)
                .default_value("glacier"),
        )
        .arg(
            Arg::with_name("Use Path Style")
                .long("with-path-style")
                .short("p")
                .takes_value(false),
        )
        .get_matches();

    let region_name = matches.value_of("Region").unwrap().to_string();
    let endpoint = matches.value_of("Endpoint").unwrap().to_string();
    let region = Region::Custom {
        region: region_name,
        endpoint,
    };

    let access_key = matches.value_of("Access key").unwrap();
    let secret_key = matches.value_of("Secret key").unwrap();
    let credentials = Credentials::new(Some(access_key), Some(secret_key), None, None, None)?;

    println!("Listing files...");
    let mut bucket = if matches.is_present("Use Path Style") {
        Bucket::new_with_path_style(
            matches.value_of("Bucket").unwrap(),
            region.clone(),
            credentials.clone(),
        )?
    } else {
        Bucket::new(
            matches.value_of("Bucket").unwrap(),
            region.clone(),
            credentials.clone(),
        )?
    };
    if matches.value_of("Storage Class").unwrap() == "glacier" {
        bucket.add_header("x-amz-storage-class", "GLACIER");
    }
    let bucket_clone = bucket.clone();

    let folder = matches.value_of("Folder").unwrap();

    let bucket_objects = read_elements_from_bucket(&bucket_clone)
        .await
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
    println!("Remote files listed.");

    let conf = Config {
        bucket: bucket_clone,
        folder_base: PathBuf::from(folder),
    };
    conf.update_bucket(&bucket_objects, vec![]).await?;
    println!("Sync complete");

    println!("Clearing old dangling parts...");
    for result in bucket.list_multiparts_uploads(None, None).await? {
        for upload in result.uploads {
            if let Err(_) = bucket.abort_upload(&upload.key, &upload.id).await {
                println!("Couldn't abort multipart upload for '{}'", upload.key);
            }
        }
    }

    println!("Old parts cleared");
    Ok(())
}
