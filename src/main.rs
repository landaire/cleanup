use atomic::AtomicUsize;

use rayon::{prelude::*, Scope};
use sha1::{Digest, Sha1};
use std::collections::HashSet;
use std::fs;
use std::io::Result;
use std::path::PathBuf;
use std::sync::{atomic, RwLock};
use structopt::StructOpt;

#[derive(Debug, StructOpt)]
#[structopt(name = "cleanup", about = "Recursively cleanup a directory")]
struct Opt {
    /// Input directory
    #[structopt(parse(from_os_str))]
    input: PathBuf,
}

fn main() -> Result<()> {
    let opt = Opt::from_args();
    let deleted_count = atomic::AtomicUsize::new(0);

    rayon::scope(|s| {
        s.spawn(|s| {
            process_directory(opt.input, &deleted_count, s);
        });
    });

    println!(
        "Deleted {} files",
        deleted_count.load(atomic::Ordering::Acquire)
    );

    Ok(())
}

/// Processes the files in the given directory. When a subdirectory is encountered a new
/// task is spawned to handle that directory.
fn process_directory<'a, 'b>(
    dir: PathBuf,
    deleted_file_count: &'a AtomicUsize,
    scope: &'b Scope<'a>,
) {
    println!("{:?}", dir);
    let hashes = RwLock::new(HashSet::new());
    for entry in fs::read_dir(dir).expect("failed to read dir") {
        let mut hasher = Sha1::new();
        let entry = entry.as_ref().expect("failed to parse entry");
        let path = entry.path();

        let file_type = entry.file_type().expect("failed to get file type");
        if file_type.is_dir() && !file_type.is_symlink() {
            scope.spawn(move |s| {
                process_directory(path, deleted_file_count, s);
            });
            continue;
        }

        // process input message
        hasher.update(fs::read(&path).expect("failed to read file").as_slice());

        // acquire hash digest in the form of GenericArray,
        // which in this case ivalent to [u8; 20]
        let result = hasher.finalize();
        if hashes.read().unwrap().contains(&result) {
            deleted_file_count.fetch_add(1, atomic::Ordering::AcqRel);
            fs::remove_file(&path).expect("failed to remove file");
        } else {
            hashes.write().unwrap().insert(result);
        }
    }
}
