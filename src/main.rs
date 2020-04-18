use std::fmt::Debug;
use std::path::{Path, PathBuf};
use std::ffi::OsString;
use std::collections::HashMap;

use chrono::{DateTime, Utc};
use s3::credentials::Credentials;
use s3::error::S3Error;
use s3::region::Region;
use s3::bucket::Bucket;
use clap::{App, Arg};
use tokio::fs::{read, write, read_dir};
use serde::{Deserialize, Serialize};
use serde_json::{from_slice, to_vec};


const RESERVED_FILE: &'static str = ".s3s_modification_listing";

// needed for serializing/deserializing DateTime<Utc>
// cf. https://serde.rs/custom-date-format.html
mod datetime_utc_serde {
	use chrono::{DateTime, Utc, TimeZone};
	use serde::{self, Deserialize, Serializer, Deserializer};

	const FORMAT: &'static str = "%Y-%m-%d %H:%M:%S";

	pub fn serialize<S>(date: &DateTime<Utc>, serializer: S) -> Result<S::Ok, S::Error>
	where S: Serializer
	{
		let s = format!("{}", date.format(FORMAT));
		serializer.serialize_str(&s)
	}

	pub fn deserialize<'de, D>(deserializer: D) -> Result<DateTime<Utc>, D::Error>
	where D: Deserializer<'de>
	{
		let s = String::deserialize(deserializer)?;
		Utc.datetime_from_str(&s, FORMAT).map_err(serde::de::Error::custom)
	}
}

struct ObjectModificationListing (Tree<OsString, Object>);

impl ObjectModificationListing {
	async fn load(file: impl AsRef<Path> + Debug) -> Result<Self, tokio::io::Error> {
		Ok(match read(&file).await {
			Ok(x) => ObjectModificationListing(from_slice(&x).unwrap()),
			Err(e) => {
				println!("Could not find file file {:?}, creating a default file config", file);
				println!("Note: this mesage is perfectly normal if this is the first time you are running s3s against this folder.");
				ObjectModificationListing(Tree::new())
			}
		})
	}
	async fn save(&self, file: impl AsRef<Path>) -> Result<(), tokio::io::Error> {
		write(file, to_vec(&self.0).unwrap()).await
	}
	fn from_update_data(&self, folder: impl AsRef<Path>, tree: &Tree<OsString, ObjectUpdateData>) -> ObjectModificationListing {
		ObjectModificationListing(tree.transform_with_path(&|path, x| {
			match self.0.get(path).and_then(|listing_x_list|
				listing_x_list.iter()
					.filter(|listing_x| listing_x.name == x.name)
					// reuse the hash only if the modification date in the file is more recent
					// than the modification date on the fileystem
					.filter(|listing_x| listing_x.last_modification_date >= x.last_modification_date)
					.next()
					.and_then(|listing_x| Some(listing_x.clone()))
			) {
				Some(x) => x,
				None => {
					// create a new value
					let mut file_path = folder.as_ref().to_path_buf();
					for e in path {
						file_path.push(e);
					}
					file_path.push(&x.name);
					Object {
						name: x.name.clone(),
						hash: hash_file(&file_path).unwrap(),
						last_modification_date: x.last_modification_date
					}
				}
			}
		}))
	}
}

/// An abstraction for files
/// Two objects are the same if they have both the same name, and the same hash
#[derive(Debug, Clone, Eq, PartialOrd, Ord, Serialize, Deserialize)]
struct Object {
	name: OsString,
	hash: String,
	#[serde(with = "datetime_utc_serde")]
	last_modification_date: DateTime<Utc>
}

impl PartialEq for Object {
	fn eq(&self, res: &Self) -> bool {
		self.name == res.name && self.hash == res.hash
	}
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct ObjectUpdateData {
	name: OsString,
	last_modification_date: DateTime<Utc>
}


#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
struct Tree<K, V> {
	children: Vec<(K, Tree<K, V>)>,
	values: Option<Vec<V>>
}

impl<K, V> Tree<K, V> where K: PartialEq + Clone + Debug, V: Clone {
	fn new() -> Self {
		Tree {
			children: Vec::new(),
			values: None
		}
	}

	fn get_subtree(&self, path: &[&K]) -> Option<&Tree<K, V>> {
		let mut subtree = self;
		'ext: for entry in path {
			for i in 0..subtree.children.len() {
				if subtree.children[i].0 == **entry {
					subtree = &subtree.children[i].1;
					continue 'ext;
				}
			}
			// couldn't find the path in the tree
			return None;
		}
		Some(subtree)
	}

	fn create_subtree(&mut self, path: &[K]) -> &mut Tree<K, V> {
		let mut subtree = self;
		'ext: for entry in path {
			for i in 0..subtree.children.len() {
				if subtree.children[i].0 == *entry {
					// the tree already contain this path, let's go there
					subtree = &mut subtree.children[i].1;
					continue 'ext;
				}
			}

			// too bad, the path isn't there yet, let's fend for ourselves and build
			// it with our bare hands (and a little help from the compiler)
			subtree.children.push((entry.clone(), Tree::new()));
			subtree = &mut subtree.children.last_mut().unwrap().1;
		}
		subtree
	}

	fn get(&self, path: &[&K]) -> Option<&[V]> {
		self.get_subtree(path)
			.and_then(|sub|
				sub.values.as_ref()
				.and_then(|values|
					Some(values.as_ref())
				)
			)
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

	fn transform<F, W>(&self, fun: &F) -> Tree<K, W> where F: Fn(&V) -> W {
		let children = self.children
			.iter()
			.map(|(key, child)| (key.clone(), child.transform(fun)))
			.collect();
		let values = self.values
			.as_ref()
			.map(|x| x.iter().map(|e| fun(e)).collect());

		Tree {
			children,
			values
		}
	}

	fn transform_with_path_internal<'a, F, W>(&'a self, fun: &F, path: &mut Vec<&'a K>) -> Tree<K, W> where F: Fn(&[&K], &V) -> W {
		let children = self.children.iter().map(|(key, child)| {
			path.push(key);
			let res = (key.clone(), child.transform_with_path_internal(fun, path));
			path.pop();
			res
		}).collect();
		let values = self.values.as_ref().map(|x| x.iter().map(|e| fun(&path, e)).collect());

		Tree {
			children,
			values
		}
	}

	fn transform_with_path<F, W>(&self, fun: &F) -> Tree<K, W> where F: Fn(&[&K], &V) -> W {
		self.transform_with_path_internal(fun, &mut Vec::new())
	}
}

impl<K, V> Tree<K, V> where K: PartialEq + Clone + Ord, V: Clone + Ord {
	/// sort to facilitate comparisons
	fn sort(&mut self) {
		self.children.sort();
		if let Some(ref mut values) = self.values {
			values.sort();
		}
	}
}

#[derive(Debug)]
struct DifferenceTree<K, V> {
	children: Vec<(K, DifferenceTree<K, V>)>,
	new_values: Option<Vec<V>>,
	old_values: Option<Vec<V>>
}

#[derive(Debug)]
struct DifferenceTreeResult<K, V> (Option<DifferenceTree<K, V>>);

impl<K, V> From<&(Option<&Tree<K, V>>, Option<&Tree<K, V>>)> for DifferenceTreeResult<K, V>
	where K: PartialEq + Eq + std::hash::Hash + Clone, V: Clone + PartialEq {

	fn from((a, b): &(Option<&Tree<K, V>>, Option<&Tree<K, V>>)) -> Self {
		if a.is_none() && b.is_none() {
			return DifferenceTreeResult(None);
		}

		let mut children_set = HashMap::new();
		let mut old_values = Vec::new();
		let mut new_values = Vec::new();

		if let Some(a) = a {
			for e in &a.children {
				children_set.insert(&e.0, (Some(&e.1), None));
			}
			if let Some(values_a) = &a.values {
				for e in values_a {
					old_values.push(e.clone());
				}
			}
		}
		if let Some(b) = b {
			for e in &b.children {
				if let Some(mut v) = children_set.get_mut(&e.0) {
					v.1 = Some(&e.1);
				} else {
					children_set.insert(&e.0, (None, Some(&e.1)));
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
			if let DifferenceTreeResult(Some(x)) = children_set.get(i).unwrap().into() {
				children.push(((*i).clone(), x));
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
			old_values
		}))
	}
}

impl DifferenceTree<OsString, Object> {
	fn update_bucket_with_prefix<'a>(&'a self, bucket: &'a Bucket, prefix: &'a Path, base_dir: &'a Path) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), S3Error>> + 'a>> {
		Box::pin(async move {
			let mut current_dir = base_dir.to_path_buf();
			current_dir.push(prefix);
			// depth-first sync: sync children before values
			for (key, child) in &self.children {
				let mut prefix = prefix.to_path_buf();
				prefix.push(&key);
				child.update_bucket_with_prefix(bucket, &prefix, base_dir).await?;
			}
			if let Some(old_values) = &self.old_values {
				for i in old_values {
					let mut path = prefix.to_path_buf();
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
	let mut res = Tree::new();

	let mut is_finished = false;
	let mut continuation_token = None;
	while !is_finished {
		let bucket_res_list = bucket.list_page("".into(), None, continuation_token).await?;
		let list_obj = &bucket_res_list.0;

		let mut keys = Vec::with_capacity(list_obj.contents.len());
		for e in &list_obj.contents {
			let mut paths: Vec<OsString> = e.key.split("/").map(|e| e.into()).collect();
			let file_name = paths.pop().unwrap();
			keys.push((paths,
				Object {
					name: file_name.into(),
					hash: e.e_tag[1..e.e_tag.len()-1].to_string(),
					// doesn't matter for comparisons
					last_modification_date:
						DateTime::parse_from_rfc3339(&e.last_modified).unwrap().into()
				}));
		}

		if keys.len() > 0 {
			res.add_values_with_path(&mut keys);
		}

		is_finished  = !list_obj.is_truncated;
		continuation_token = list_obj.next_continuation_token.clone();
	}
	res.sort();
	Ok(res)
}

/// read a file and compute its hash
fn hash_file(path: impl AsRef<Path>) -> Result<String, std::io::Error> {
	let file_content = std::fs::read(&path)?;
	Ok(format!("{:x}", md5::compute(&file_content)))
}

/// Iterate over all the objects in the folder, and generate a tree of their hierarchy
async fn read_update_data_from_folder(folder: &Path) -> Result<Tree<OsString, ObjectUpdateData>, tokio::io::Error> {
	let mut res = Tree::new();

	// list of paths to visit
	let mut to_visit: Vec<PathBuf> = vec![folder.to_path_buf()];
	while to_visit.len() > 0 {
		let current_path = to_visit.pop().unwrap();
		let current_path_splitted = current_path.strip_prefix(folder)
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
				let name = entry.path().file_name().unwrap().to_os_string();
				if name == RESERVED_FILE {
					continue;
				}
				list_paths.push(ObjectUpdateData {
					name,
					last_modification_date: DateTime::from(tokio::fs::metadata(&entry.path()).await?.modified().unwrap())
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

/// Iterate over all the objects in the folder, and generate a tree of their hierarchy, using caching
async fn read_elements_from_folder(folder: &Path) -> Result<Tree<OsString, Object>, tokio::io::Error> {
	let mut file_listing = folder.to_path_buf();
	file_listing.push(RESERVED_FILE);
	let old_listing = ObjectModificationListing::load(&file_listing).await?;

	let update_data = read_update_data_from_folder(folder).await?;

	let updated_listing = old_listing.from_update_data(folder, &update_data);

	updated_listing.save(&file_listing).await?;

	Ok(updated_listing.0)
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

	println!("Listing foreign files...");
	let bucket_objects = read_elements_from_bucket(&bucket).await?;


	println!("Listing local files...");
	let folder = Path::new(matches.value_of("Folder").unwrap());
	let folder_objects = read_elements_from_folder(&folder).await.unwrap();

	println!("Computing the difference...");
	let difference = DifferenceTreeResult::from(&(Some(&bucket_objects), Some(&folder_objects)));

	if let DifferenceTreeResult(Some(d)) = difference {
		d.update_bucket(&bucket, &folder).await?;
	}
	println!("Sync complete");
	Ok(())
}