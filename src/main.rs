use std::fmt::Debug;
use std::path::{Path, PathBuf};
use std::ffi::OsString;
use std::convert::{TryFrom, TryInto};
use std::collections::HashMap;

use s3::credentials::Credentials;
use s3::error::S3Error;
use s3::region::Region;
use s3::bucket::Bucket;
use clap::{App, Arg};
use tokio::fs::{read, read_dir};


/// An abstracton for files
/// Two objects are the same if they have both the same name, and the same hash
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct Object {
    name: OsString,
    hash: String
}


#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
struct Tree<K, V> {
    key: K,
    children: Vec<Tree<K, V>>,
    values: Option<Vec<V>>
}

impl<K, V> Tree<K, V> where K: PartialEq + Clone + Debug, V: Clone {
    fn new(root_key: impl Into<K>) -> Self {
        Tree {
            key: root_key.into(),
            children: Vec::new(),
            values: None
        }
    }

    fn obtain_subtree(&mut self, path: &[K]) -> &mut Tree<K, V> {
        let mut subtree = self;
        'ext: for entry in path {
            for i in 0..subtree.children.len() {
                if subtree.children[i].key == *entry {
                    // the tree already contain this path, let's go there
                    subtree = &mut subtree.children[i];
                    continue 'ext; 
                }
            }

            // too bad, the path isn't there yet, let's fend for ourselves and build
            // it with our bare hands (and a little help from the compiler)
            subtree.children.push(Tree::new(entry.clone()));
            subtree = subtree.children.last_mut().unwrap();
        }
        subtree
    }

    fn add_values(&mut self, path: &[K], mut objects: &mut Vec<V>) {
        let subtree = self.obtain_subtree(path);
        let values = subtree.values.get_or_insert(Vec::new());
        values.append(&mut objects);
        // sort to facilitate comparisons;
    }
}

impl<K, V> Tree<K, V> where K: PartialEq + Clone + Ord, V: Clone + Ord {
    /// sort to facilitate 
    fn sort(&mut self) {
        self.children.sort();
        if let Some(ref mut values) = self.values {
            values.sort();
        }
    }
}

#[derive(Debug)]
struct DifferenceTree<K, V> {
    key: K,
    children: Vec<DifferenceTree<K, V>>,
    new_values: Option<Vec<V>>,
    old_values: Option<Vec<V>>
}

#[derive(Debug)]
struct DifferenceTreeResult<K, V> (Option<DifferenceTree<K, V>>);

#[derive(Debug)]
struct KeyError;

impl<K, V> TryFrom<&(Option<&Tree<K, V>>, Option<&Tree<K, V>>)> for DifferenceTreeResult<K, V>
    where K: PartialEq + Eq + std::hash::Hash + Clone, V: Clone + PartialEq {
    type Error = KeyError;

    fn try_from((a, b): &(Option<&Tree<K, V>>, Option<&Tree<K, V>>)) -> Result<Self, Self::Error> {
        if a.is_none() && b.is_none() {
            return Ok(DifferenceTreeResult(None));
        }
        // sanity check
        if a.is_some() && b.is_some() && a.unwrap().key != b.unwrap().key {
            return Err(KeyError);
        }

        let mut children_set = HashMap::new();
        let mut old_values = Vec::new();
        let mut new_values = Vec::new();

        if let Some(a) = a {
            for e in &a.children {
                children_set.insert(&e.key, (Some(e), None));
            }
            if let Some(values_a) = &a.values {
                for e in values_a {
                    old_values.push(e.clone());
                }
            }
        }
        if let Some(b) = b {
            for e in &b.children {
                if let Some(mut v) = children_set.get_mut(&e.key) {
                    v.1 = Some(e);
                } else {
                    children_set.insert(&e.key, (None, Some(e)));
                }
            }

            if let Some(values_b) = &b.values {
                for e in values_b {
                    if let Some((i, _)) = old_values.iter()
                        .enumerate()
                        .filter(|(_, v)| v == &e)
                        .next() {

                        // present in both a and b, let's forget about it
                        old_values.remove(i);
                    } else {
                        // present only in b, let's add it to new_values
                        new_values.push(e.clone());
                    }
                }
            }
        }

        let mut children = Vec::new();
        for i in children_set.keys() {
            // compare the two children but delete the common parts
            match children_set.get(i).unwrap().try_into() {
                Ok(DifferenceTreeResult(Some(x))) => children.push(x),
                Err(KeyError) => return Err(KeyError),
                _ => {}
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
            return Ok(DifferenceTreeResult(None));
        }

        Ok(DifferenceTreeResult(Some(DifferenceTree {
            key: a.map(|x| x.key.clone()).unwrap_or_else(|| b.unwrap().key.clone()),
            children,
            new_values,
            old_values
        })))
    }
}

impl DifferenceTree<OsString, Object> {
    fn update_bucket_with_prefix<'a>(&'a self, bucket: &'a Bucket, prefix: &'a Path, base_dir: &'a Path) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), S3Error>> + 'a>> {
        Box::pin(async move {
            let mut current_dir = base_dir.to_path_buf();
            current_dir.push(prefix);
            // depth-first sync: sync children before values
            for i in &self.children {
                let mut prefix = prefix.to_path_buf();
                prefix.push(&i.key);
                i.update_bucket_with_prefix(bucket, &prefix, base_dir).await?;
            }
            if let Some(old_values) = &self.old_values {
                for i in old_values {
                    let mut path = current_dir.clone();
                    path.push(&i.name);
                    // delete the file in the bucket
                    println!("Deleting {:?}", path);
                    bucket.delete_object(path.to_str().unwrap()).await?;
                }
            }
            if let Some(new_values) = &self.new_values {
                for i in new_values {
                    let mut path = prefix.to_path_buf();
                    path.push(&i.name);
                    let mut local_path = current_dir.to_path_buf();
                    local_path.push(&i.name);
                    // upload the local file
                    println!("Uploading {:?}", path);
                    let status_code = bucket.put_object_stream(local_path.to_str().unwrap(), path.to_str().unwrap()).await?;
                    assert_eq!(status_code, 200)
                }
            }
            Ok(())
        })
    }

    async fn update_bucket(&self, bucket: &Bucket, base_dir: &Path) -> Result<(), S3Error> {
        self.update_bucket_with_prefix(bucket, &Path::new(""), base_dir).await
    }
}

/// Iterate over all the objects in the bucket, and generate a tree of their hierarchy
async fn read_elements_from_bucket(bucket: &Bucket) -> Result<Tree<OsString, Object>, S3Error> {
    let mut res = Tree::new("/");

    // list of paths to visit
    let mut to_visit: Vec<String> = vec![String::from("")];
    while to_visit.len() > 0 {

        let current_path = to_visit.pop().unwrap();
        let mut current_path_splitted = current_path.split("/")
            .map(|x| x.to_string().into())
            .collect::<Vec<OsString>>();
        // delete the empty string at the end of the path
        current_path_splitted.pop();

        let mut is_finished = false;
        let mut continuation_token = None;
        while !is_finished {
            let bucket_res_list = bucket.list_page(current_path.clone(), Some("/".to_string()), continuation_token).await?;
            let list_obj = &bucket_res_list.0;

            let mut keys: Vec<Object> = list_obj.contents
                .iter()
                .map(|e| (&e.key, e.e_tag.clone()))
                // only keep the file name
                .map(|(e, v)| (e.rsplit("/").map(|v| v.to_string()).next().unwrap(), v))
                // filter out directories
                .filter(|(e, _)| e.len() > 0)
                // convert to an Object
                .map(|(e, v)|
                    Object {
                        name: e.into(),
                        hash: v
                    }
                ).collect();

            if keys.len() > 0 {
                res.add_values(current_path_splitted.as_slice(), &mut keys);
            }

            if let Some(common_prefixes) = &list_obj.common_prefixes {
                let mut common_prefixes = common_prefixes.iter().map(|x| x.prefix.clone()).collect();
                to_visit.append(&mut common_prefixes);
            }

            is_finished  = !list_obj.is_truncated;
            continuation_token = list_obj.next_continuation_token.clone();
        }
    }
    res.sort();
    Ok(res)
}

/// Iterate over all the objects in the folder, and generate a tree of their hierarchy
async fn read_elements_from_folder(path: &Path) -> Result<Tree<OsString, Object>, tokio::io::Error> {
    let mut res = Tree::new("/");

    // list of paths to visit
    let mut to_visit: Vec<PathBuf> = vec![path.to_path_buf()];
    while to_visit.len() > 0 {
        let current_path = to_visit.pop().unwrap();
        let current_path_splitted = current_path.strip_prefix(path)
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
            } else {
                // read file and compute its hash
                let file_content = read(&entry.path()).await?;
                let file_hash = format!("{:x}", md5::compute(&file_content));
                list_paths.push(Object {
                    name: entry.path().file_name().unwrap().to_os_string(),
                    hash: file_hash
                });
            }
        }

        if list_paths.len() > 0 {
            res.add_values(current_path_splitted.as_slice(), &mut list_paths);
        }
    }
    res.sort();
    Ok(res)
}


#[tokio::main]
async fn main()-> Result<(), S3Error> {
    let matches = App::new("s3s")
        .version("0.1")
        .author("Simon Thoby <git@nightmared.fr>")
        .about("Sync a folder to a s3 bucket")
        .arg(Arg::with_name("Secret key")
            .short("s")
            .long("secret-key")
            .takes_value(true)
            .required(true))
        .arg(Arg::with_name("Access key")
            .short("a")
            .long("access-key")
            .takes_value(true)
            .required(true))
        .arg(Arg::with_name("Region")
            .long("region")
            .takes_value(true)
            .default_value("fr-par"))
        .arg(Arg::with_name("Endpoint")
            .long("endpoint")
            .takes_value(true)
            .default_value("https://s3.fr-par.scw.cloud"))
        .arg(Arg::with_name("Bucket")
            .short("b")
            .long("bucket")
            .takes_value(true)
            .required(true))
        .arg(Arg::with_name("Folder")
            .short("f")
            .long("folder")
            .takes_value(true)
            .required(true))
        .get_matches();

    let region_name = matches.value_of("Region").unwrap().to_string();
    let endpoint = matches.value_of("Endpoint").unwrap().to_string();
    let region = Region::Custom { region: region_name, endpoint };

    let access_key = String::from(matches.value_of("Access key").unwrap());
    let secret_key = String::from(matches.value_of("Secret key").unwrap());
    let credentials = Credentials::new(Some(access_key), Some(secret_key), None, None).await?;

    let bucket = Bucket::new(matches.value_of("Bucket").unwrap(), region, credentials)?;

    let bucket_objects = read_elements_from_bucket(&bucket).await?;


    println!("{:?}", bucket_objects);

    let folder = Path::new(matches.value_of("Folder").unwrap());
    let folder_objects = read_elements_from_folder(&folder).await.unwrap();

    let difference = DifferenceTreeResult::try_from(&(Some(&bucket_objects), Some(&folder_objects))).unwrap();

    if let DifferenceTreeResult(Some(d)) = difference {
        d.update_bucket(&bucket, &folder).await?;
    }
    Ok(())
}