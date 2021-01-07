use std::collections::HashMap;
use std::ffi::OsString;
use std::fmt::Debug;
use std::future::Future;
use std::hash::Hash;
use std::path::{Path, PathBuf};
use std::pin::Pin;

use anyhow::Result;
use chrono::{DateTime, Utc};
use clap::{App, Arg};
use http::header::{HeaderMap, HeaderName, HeaderValue};
use rayon::prelude::*;
use s3::bucket::Bucket;
use s3::creds::Credentials;
use s3::region::Region;
use serde::{Deserialize, Serialize};
use serde_json::{from_slice, to_vec};
use serde_with::serde_as;
use sha2::{Digest, Sha256};
use tokio::fs::{read, read_dir, write, File};
use tokio::io::AsyncReadExt;

mod select;
use select::{select_vec, selector, Selector};

const RESERVED_FILE: &'static str = ".s3s_modification_listing";

// needed for serializing/deserializing DateTime<Utc>
// cf. https://serde.rs/custom-date-format.html
mod datetime_utc_serde {
    use chrono::{DateTime, TimeZone, Utc};
    use serde::{self, Deserialize, Deserializer, Serializer};

    const FORMAT: &'static str = "%Y-%m-%d %H:%M:%S";

    pub fn serialize<S>(date: &DateTime<Utc>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let s = format!("{}", date.format(FORMAT));
        serializer.serialize_str(&s)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<DateTime<Utc>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        Utc.datetime_from_str(&s, FORMAT)
            .map_err(serde::de::Error::custom)
    }
}

struct ObjectModificationListing(Tree<OsString, Object>);

impl ObjectModificationListing {
    async fn load(file: impl AsRef<Path> + Debug) -> Result<Self> {
        match read(&file).await.map(|x| {
            from_slice(&x).map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))
        }) {
            Ok(Ok(x)) => return Ok(ObjectModificationListing(x)),
            Ok(Err(e)) => {
                println!("Could not parse the file {:?}: {:?}", file, e);
            }
            Err(_) => {
                println!(
                    "Could not find file {:?}, creating a default file config",
                    file
                );
                println!("Note: this message is perfectly normal if this is the first time you are running s3s against this folder.");
            }
        }
        Ok(ObjectModificationListing(Tree::new()))
    }

    async fn save(&self, file: impl AsRef<Path>) -> Result<(), tokio::io::Error> {
        write(file, to_vec(&self.0).unwrap()).await
    }

    async fn from_update_data<'a>(
        &'a self,
        folder: &'a Path,
        tree: &'a Tree<OsString, ObjectUpdateData>,
    ) -> ObjectModificationListing {
        ObjectModificationListing(
            tree.transform_with_path(&|path, obj| path_transform(folder, self, path, obj))
                .await,
        )
    }
}

fn path_transform<'a>(
    folder: &'a Path,
    source_tree: &'a ObjectModificationListing,
    path: Vec<OsString>,
    obj: &'a ObjectUpdateData,
) -> Pin<Box<dyn Future<Output = Object> + Sync + Send + 'a>> {
    Box::pin(async move {
        let mut res = None;
        // the logic is the following: check if the same version of the file is present across the list of
        // files present in the same folder
        if let Some(objs) = source_tree.0.get(&path) {
            // reuse the hash only if the cache file is as old as the one on the fileystem
            res = objs
                .iter()
                .filter(|cached_obj| {
                    cached_obj.name == obj.name
                        && cached_obj.last_modification_date < obj.last_modification_date
                })
                // take the first entry, as there can only be a single entry for a
                // given name
                .next();
        }

        match res {
            Some(res) => res.clone(),
            None => {
                // create a new value
                let mut file_path = folder.to_path_buf();
                for e in &*path {
                    file_path.push(e);
                }
                file_path.push(&obj.name);
                Object {
                    name: obj.name.clone(),
                    sha256: hash_file(&file_path).await.unwrap(),
                    last_modification_date: obj.last_modification_date,
                }
            }
        }
    })
}

/// An abstraction for files
/// Two objects are the same if they have both the same name, and the same hash
#[derive(Debug, Clone, Eq, PartialOrd, Ord, Serialize, Deserialize)]
struct Object {
    name: OsString,
    sha256: String,
    #[serde(with = "datetime_utc_serde")]
    last_modification_date: DateTime<Utc>,
}

impl PartialEq for Object {
    fn eq(&self, res: &Self) -> bool {
        self.name == res.name && self.sha256 == res.sha256
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct ObjectUpdateData {
    name: OsString,
    last_modification_date: DateTime<Utc>,
}

#[serde_as]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct Tree<K, V>
where
    K: Eq + Hash + Serialize + for<'e> Deserialize<'e>,
    V: Eq,
{
    #[serde_as(as = "Vec<(_, _)>")]
    children: HashMap<K, Tree<K, V>>,
    values: Option<Vec<V>>,
}

impl<K, V> Tree<K, V>
where
    K: Eq + Hash + Clone + Serialize + for<'e> Deserialize<'e> + Send + Sync,
    V: Eq + Clone + Sync,
{
    fn new() -> Self {
        Tree {
            children: HashMap::new(),
            values: None,
        }
    }

    fn get_subtree(&self, path: &[K]) -> Option<&Tree<K, V>> {
        let mut subtree = self;
        for entry in path {
            match subtree.children.get(entry) {
                Some(tree) => {
                    subtree = tree;
                }
                // couldn't find the path in the tree
                None => return None,
            }
        }
        Some(subtree)
    }

    fn create_subtree(&mut self, path: &[K]) -> &mut Tree<K, V> {
        let mut subtree = self;
        for entry in path {
            if subtree.children.get(entry) == None {
                // too bad, the path isn't there yet, let's fend for ourselves and build
                // it with our bare hands (and a little help from the compiler)
                subtree.children.insert(entry.clone(), Tree::new());
            }

            subtree = subtree.children.get_mut(entry).unwrap();
        }
        subtree
    }

    fn get(&self, path: &[K]) -> Option<&[V]> {
        self.get_subtree(path)
            .and_then(|sub| sub.values.as_ref().and_then(|values| Some(values.as_ref())))
    }

    fn add_values(&mut self, path: &[K], mut objects: &mut Vec<V>) {
        let subtree = self.create_subtree(path);
        let values = subtree.values.get_or_insert(Vec::new());
        values.append(&mut objects);
    }

    fn add_values_with_path(&mut self, objects: &mut Vec<(Vec<K>, V)>) {
        while let Some(e) = objects.pop() {
            let subtree = self.create_subtree(&e.0);
            let values = subtree.values.get_or_insert(Vec::new());
            values.push(e.1);
        }
    }

    fn transform_with_path_internal<'a, F, W>(
        &'a self,
        fun: &'a F,
        path: Vec<K>,
    ) -> Pin<Box<dyn Future<Output = Tree<K, W>> + 'a>>
    where
        F: Fn(Vec<K>, &'a V) -> Pin<Box<dyn Future<Output = W> + 'a + Sync + Send>> + Sync,
        K: Hash,
        W: Eq + Send,
    {
        Box::pin(async move {
            let mut children = HashMap::with_capacity(self.children.len());
            for (key, child) in &self.children {
                let mut path = path.clone();
                path.push(key.clone());
                let res = child.transform_with_path_internal(fun, path).await;
                children.insert(key.clone(), res);
            }

            let values_fut: Option<Vec<Pin<Box<dyn Future<Output = W> + Sync + Send>>>> =
                self.values.as_ref().map(|self_values| {
                    self_values
                        .as_slice()
                        .par_iter()
                        .map(|x| fun(path.clone(), &x))
                        .collect()
                });

            let mut values = None;
            if let Some(mut values_fut) = values_fut {
                let mut res = Vec::with_capacity(values_fut.len());
                select_vec(&mut values_fut, &mut res)
                    .await
                    .expect("Couldn't process file informations");
                values = Some(res);
            }

            Tree { children, values }
        })
    }

    fn transform_with_path<'a, F, W>(
        &'a self,
        fun: &'a F,
    ) -> Pin<Box<dyn Future<Output = Tree<K, W>> + 'a>>
    where
        F: Fn(Vec<K>, &'a V) -> Pin<Box<dyn Future<Output = W> + 'a + Sync + Send>> + Sync,
        W: Eq + Send,
    {
        self.transform_with_path_internal(fun, Vec::new())
    }
}

#[derive(Debug)]
struct DifferenceTree<K, V> {
    children: HashMap<K, DifferenceTree<K, V>>,
    new_values: Option<Vec<V>>,
    old_values: Option<Vec<V>>,
}

#[derive(Debug)]
struct DifferenceTreeResult<K, V>(Option<DifferenceTree<K, V>>);

impl<K, V> From<&(Option<&Tree<K, V>>, Option<&Tree<K, V>>)> for DifferenceTreeResult<K, V>
where
    K: Eq + std::hash::Hash + Clone + Serialize + for<'e> Deserialize<'e>,
    V: Clone + Eq,
{
    fn from((a, b): &(Option<&Tree<K, V>>, Option<&Tree<K, V>>)) -> Self {
        if a.is_none() && b.is_none() {
            return DifferenceTreeResult(None);
        }

        let mut children_set = HashMap::new();
        let mut old_values = Vec::new();
        let mut new_values = Vec::new();

        if let Some(a) = a {
            for (child_key, child_value) in &a.children {
                children_set.insert(child_key, (Some(child_value), None));
            }

            if let Some(values_a) = &a.values {
                for e in values_a {
                    old_values.push(e.clone());
                }
            }
        }

        if let Some(b) = b {
            for (child_key, child_value) in &b.children {
                if let Some(mut v) = children_set.get_mut(&child_key) {
                    v.1 = Some(child_value);
                } else {
                    children_set.insert(child_key, (None, Some(child_value)));
                }
            }

            if let Some(values_b) = &b.values {
                for e in values_b {
                    if let Some((i, _)) = old_values
                        .iter()
                        .enumerate()
                        .filter(|(_, v)| v == &e)
                        .next()
                    {
                        // present in both a and b, let's forget about it
                        old_values.remove(i);
                    } else {
                        // present only in b, let's add it to new_values
                        new_values.push(e.clone());
                    }
                }
            }
        }

        let mut children = HashMap::new();
        for i in children_set.keys() {
            // compare the two children but delete the common parts
            if let DifferenceTreeResult(Some(x)) = children_set.get(i).unwrap().into() {
                children.insert((*i).clone(), x);
            }
        }

        let old_values = if old_values.len() > 0 {
            Some(old_values)
        } else {
            None
        };
        let new_values = if new_values.len() > 0 {
            Some(new_values)
        } else {
            None
        };

        // if this subtree has not been altered, let's forget about it
        if children.len() == 0 && new_values.is_none() && old_values.is_none() {
            return DifferenceTreeResult(None);
        }

        DifferenceTreeResult(Some(DifferenceTree {
            children,
            new_values,
            old_values,
        }))
    }
}

async fn delete_object(bucket: &Bucket, path: PathBuf) -> Result<()> {
    println!("Deleting {:?}", path);
    bucket
        .delete_object(path.to_str().unwrap())
        .await
        .map(|_| ())
}

async fn upload_object(
    bucket: &Bucket,
    sha256: HeaderValue,
    local_path: PathBuf,
    path: PathBuf,
) -> Result<()> {
    println!("Uploading {:?}", path);
    let mut custom_headers = HeaderMap::new();
    custom_headers.insert(HeaderName::from_static("x-amz-meta-sha256"), sha256);
    let status_code = bucket
        .put_object_stream_with_headers(
            local_path.to_str().unwrap(),
            path.to_str().unwrap(),
            Some(custom_headers),
        )
        .await?;
    assert_eq!(status_code, 200);
    Ok(())
}

impl DifferenceTree<OsString, Object> {
    fn update_bucket_with_prefix<'a>(
        &'a self,
        bucket: &'a Bucket,
        prefix: PathBuf,
        base_dir: &'a Path,
        selector: &Selector<Pin<Box<dyn Future<Output = Result<()>> + 'a>>>,
    ) -> Result<()> {
        let mut current_dir = base_dir.to_path_buf();
        current_dir.push(prefix.as_path());

        for (key, child) in &self.children {
            let mut prefix = prefix.clone();
            prefix.push(&key);
            child.update_bucket_with_prefix(bucket, prefix, base_dir, &selector)?;
        }

        let old_value = vec![];
        let old_values = if let Some(old_values) = &self.old_values {
            old_values
        } else {
            &old_value
        };

        let mut new_values = if let Some(new_values) = &self.new_values {
            new_values.clone()
        } else {
            vec![]
        };

        for i in old_values {
            let mut path = prefix.to_path_buf();
            path.push(&i.name);
            if let Some((pos, new_val)) = new_values
                .iter()
                .enumerate()
                .filter(|(_, val)| val.name == i.name)
                .map(|(i, val)| (i, val.clone()))
                .next()
            {
                new_values.swap_remove(pos);
                let mut local_path = current_dir.to_path_buf();
                local_path.push(&i.name);
                let sha256 = HeaderValue::from_bytes(new_val.sha256.as_bytes())?;
                selector.add(Box::pin(async move {
                    delete_object(bucket, path.clone()).await?;
                    upload_object(bucket, sha256, local_path, path).await
                }));
            } else {
                // delete the file in the bucket
                selector.add(Box::pin(delete_object(bucket, path)));
            }
        }
        for i in new_values {
            let mut path = prefix.to_path_buf();
            path.push(&i.name);
            let mut local_path = current_dir.to_path_buf();
            local_path.push(&i.name);
            let sha256 = HeaderValue::from_bytes(i.sha256.as_bytes())?;
            selector.add(Box::pin(upload_object(bucket, sha256, local_path, path)));
        }

        Ok(())
    }

    async fn update_bucket(&self, bucket: &Bucket, base_dir: &Path) -> Result<()> {
        let selector = selector(vec![], 5);
        self.update_bucket_with_prefix(bucket, PathBuf::new(), base_dir, &selector)?;
        selector.await
    }
}

/// Iterate over all the objects in the bucket, and generate a tree of their hierarchy
async fn read_elements_from_bucket(bucket: &Bucket) -> Result<Tree<OsString, Object>> {
    let mut res = Tree::new();

    let bucket_res_list = bucket.list("".into(), None).await?;

    // TODO: perform head requests in //
    for obj_list in bucket_res_list {
        let mut keys = Vec::with_capacity(obj_list.contents.len());
        for e in &obj_list.contents {
            let mut paths: Vec<OsString> = e.key.split("/").map(|e| e.into()).collect();
            let file_name = paths.pop().unwrap();
            let (object, _) = bucket.head_object(&e.key).await?;
            let sha256 = if let Some(ref metadata) = object.metadata {
                metadata.get("sha256").map(|x| x.as_str()).unwrap_or("")
            } else {
                ""
            };

            keys.push((
                paths,
                Object {
                    name: file_name.into(),
                    sha256: sha256.to_string(),
                    // doesn't matter for comparisons
                    last_modification_date: DateTime::parse_from_rfc3339(&e.last_modified)
                        .unwrap()
                        .into(),
                },
            ));
        }

        if keys.len() > 0 {
            res.add_values_with_path(&mut keys);
        }
    }

    Ok(res)
}

/// read a file and compute its hash
async fn hash_file(path: impl AsRef<Path>) -> Result<String, std::io::Error> {
    let mut fd = File::open(path).await?;
    let mut digest = Sha256::new();
    let mut buf = vec![0; 4096];
    loop {
        let read = fd.read(&mut buf).await?;
        if read == 0 {
            return Ok(format!("{:x}", digest.finalize()));
        }

        // we assume the sha256 operation to be fast enought on 4K blocks for its impact to be
        // reasonable on the async scheduler
        digest.update(&buf[0..read]);
    }
}

/// Iterate over all the objects in the folder, and generate a tree of their hierarchy
async fn read_update_data_from_folder(
    folder: &Path,
) -> Result<Tree<OsString, ObjectUpdateData>, tokio::io::Error> {
    let mut res = Tree::new();

    // list of paths to visit
    let mut to_visit: Vec<PathBuf> = vec![folder.to_path_buf()];
    while to_visit.len() > 0 {
        let current_path = to_visit.pop().unwrap();
        let current_path_splitted = current_path
            .strip_prefix(folder)
            .unwrap()
            .components()
            .map(|x| x.as_os_str().to_os_string())
            .collect::<Vec<OsString>>();

        let mut folder_res_list = read_dir(current_path).await?;
        let mut list_paths = Vec::new();
        while let Some(entry) = folder_res_list.next_entry().await? {
            let file_type = entry.file_type().await?;

            // TODO: symlinks handling
            if file_type.is_dir() {
                to_visit.push(entry.path().to_path_buf());
            } else if file_type.is_file() {
                let name = entry.path().file_name().unwrap().to_os_string();
                if name == RESERVED_FILE {
                    continue;
                }
                list_paths.push(ObjectUpdateData {
                    name,
                    last_modification_date: DateTime::from(
                        tokio::fs::metadata(&entry.path())
                            .await?
                            .modified()
                            .unwrap(),
                    ),
                });
            }
        }

        if list_paths.len() > 0 {
            res.add_values(current_path_splitted.as_slice(), &mut list_paths);
        }
    }

    Ok(res)
}

/// Iterate over all the objects in the folder, and generate a tree of their hierarchy, using caching
async fn read_elements_from_folder(folder: &Path) -> Result<Tree<OsString, Object>> {
    let mut file_listing = folder.to_path_buf();
    file_listing.push(RESERVED_FILE);
    let old_listing = ObjectModificationListing::load(&file_listing).await?;

    let update_data = read_update_data_from_folder(folder).await?;

    let updated_listing = old_listing.from_update_data(folder, &update_data).await;

    updated_listing.save(&file_listing).await?;

    Ok(updated_listing.0)
}

#[tokio::main]
async fn main() -> Result<()> {
    let matches = App::new("s3s")
        .version("0.1.7")
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
    let mut bucket = Bucket::new_with_path_style(
        matches.value_of("Bucket").unwrap(),
        region.clone(),
        credentials.clone(),
    )?;
    if matches.value_of("Storage Class").unwrap() == "glacier" {
        bucket.add_header("x-amz-storage-class", "GLACIER");
    }
    let bucket_clone = bucket.clone();

    let folder = matches.value_of("Folder").unwrap();
    let folder_path = PathBuf::from(folder);

    let listing_tasks: Vec<Pin<Box<dyn Future<Output = Result<Tree<OsString, Object>>>>>> = vec![
        Box::pin(async move {
            let res = read_elements_from_folder(folder_path.as_path()).await?;
            println!("Local files listed.");
            Ok(res)
        }),
        Box::pin(async move {
            let res = read_elements_from_bucket(&bucket_clone)
                .await
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
            println!("Remote files listed.");
            Ok(res)
        }),
    ];
    let res = futures::future::try_join_all(listing_tasks).await?;
    let (folder_objects, bucket_objects) = match &*res {
        [a, b] => (a, b),
        _ => panic!("Unknown error occured"),
    };

    println!("Computing the difference...");
    let difference = DifferenceTreeResult::from(&(Some(bucket_objects), Some(folder_objects)));

    if let DifferenceTreeResult(Some(d)) = difference {
        d.update_bucket(&bucket, &Path::new(folder)).await?;
    }
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
