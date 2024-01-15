use std::path::PathBuf;
use thiserror::Error;
use todo_fs::db::Db;

extern crate todo_fs;

#[derive(Debug, Error)]
enum ArgParseError {
    #[error("no argument after --db-path")]
    DbPathArgNotProvided,
    #[error("--db-path not provided")]
    DbPathNotProvided,
}

struct Args {
    db_path: PathBuf,
    other_args: Vec<String>,
}

impl Args {
    fn parse(mut it: impl Iterator<Item = String>) -> Result<Args, ArgParseError> {
        let mut db_path = None;
        let mut other_args = Vec::new();
        while let Some(arg) = it.next() {
            match arg.as_ref() {
                "--db-path" => {
                    db_path = it
                        .next()
                        .map(Into::into)
                        .ok_or(ArgParseError::DbPathArgNotProvided)?;
                }
                _ => {
                    other_args.push(arg);
                }
            }
        }

        let db_path = db_path.ok_or(ArgParseError::DbPathNotProvided)?.into();

        Ok(Args {
            db_path,
            other_args,
        })
    }
}

fn main() {
    env_logger::init();

    let args = Args::parse(std::env::args()).expect("failed to parse arguments");
    let db = Db::new(args.db_path).expect("failed to initialize db");

    todo_fs::fuse::run_fuse_client(db, args.other_args.into_iter());
}
