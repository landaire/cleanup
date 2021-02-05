use atomic::AtomicUsize;

use rayon::{prelude::*, Scope};
use sha1::{Digest, Sha1};
use std::collections::HashMap;
use std::fs;
use std::io::Result;
use std::path::PathBuf;
use std::sync::{atomic, RwLock};
use structopt::StructOpt;

#[derive(Debug, StructOpt)]
#[structopt(
    name = "cleanup",
    about = "Recursively find and remove duplicate files in a target directory"
)]
struct Opt {
    /// Do a dry run
    #[structopt(long = "dry")]
    dry: bool,

    /// Symlink duplicates to a single file (oldest file by default)
    #[structopt(long = "symlink")]
    symlink: bool,

    /// Input directory
    #[structopt(parse(from_os_str))]
    input: PathBuf,
}

fn main() -> Result<()> {
    let opt = Opt::from_args();
    let deleted_count = atomic::AtomicUsize::new(0);

    let input = opt.input.clone();
    rayon::scope(|s| {
        s.spawn(|s| {
            process_directory(input, &deleted_count, &opt, s);
        });
    });

    if opt.dry {
        println!(
            "Would have deleted {} files",
            deleted_count.load(atomic::Ordering::Relaxed)
        );
    } else {
        println!(
            "Deleted {} files",
            deleted_count.load(atomic::Ordering::Relaxed)
        );
    }

    Ok(())
}

/// Processes the files in the given directory. When a subdirectory is encountered a new
/// task is spawned to handle that directory.
fn process_directory<'a, 'b>(
    dir: PathBuf,
    deleted_file_count: &'a AtomicUsize,
    options: &'a Opt,
    scope: &'b Scope<'a>,
) {
    let file_sizes: RwLock<HashMap<u64, Vec<&fs::DirEntry>>> = RwLock::new(HashMap::new());

    // Gather file sizes
    let mut entries: Vec<_> = fs::read_dir(dir)
        .expect("failed to read dir")
        .map(|entry| entry.expect("failed to read dir entry"))
        .collect();

    entries.sort_by(|a, b| {
        let a_metadata = a.metadata().expect("failed to read entry metadata");
        let b_metadata = b.metadata().expect("failed to read entry metadata");

        let a_created = a_metadata
            .created()
            .expect("failed to read file created timestamp");
        let b_created = b_metadata
            .created()
            .expect("failed to read file created timestamp");

        a_created.cmp(&b_created)
    });

    entries.par_iter().for_each(|entry| {
        let metadata = entry.metadata().expect("failed to get entry metadata");
        let path = entry.path();

        let file_type = entry.file_type().expect("failed to get file type");
        if file_type.is_dir() && !file_type.is_symlink() {
            scope.spawn(move |s| {
                process_directory(path, deleted_file_count, options, s);
            });
        } else {
            file_sizes
                .write()
                .unwrap()
                .entry(metadata.len())
                .or_default()
                .push(entry);
        }
    });

    // Hash the files with a non-unique size
    file_sizes
        .read()
        .unwrap()
        .par_iter()
        .filter(|(_size, entries)| entries.len() > 1)
        .for_each(|(_size, entries)| {
            let mut hashes = HashMap::new();
            for entry in entries {
                let mut hasher = Sha1::new();
                let path = entry.path();
                // process input message
                hasher.update(fs::read(&path).expect("failed to read file").as_slice());

                // acquire hash digest in the form of GenericArray,
                // which in this case ivalent to [u8; 20]
                let result = hasher.finalize();
                if let Some(target_file) = hashes.get(&result) {
                    deleted_file_count.fetch_add(1, atomic::Ordering::Relaxed);

                    if !options.dry {
                        fs::remove_file(&path).expect("failed to remove file");
                        #[cfg(unix)]
                        {
                            if options.symlink {
                                std::os::unix::fs::symlink(&target_file, &path).unwrap_or_else(|e| {
                                    panic!(
                                        "failed to make a symlink from {:?} to {:?}: {}",
                                        path, target_file, e
                                    )
                                });
                            }
                        }
                    } else {
                        eprintln!("{:?} is a duplicate", path);
                    }
                } else {
                    hashes.insert(result, path);
                }
            }
        });
}
