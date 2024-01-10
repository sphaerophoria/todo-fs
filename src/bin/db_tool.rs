use std::{error::Error, fmt, path::PathBuf};
use thiserror::Error;
use todo_fs::db::{CreateItemError, Db};

extern crate todo_fs;

#[derive(Debug, Error)]
enum ArgParseError {
    #[error("db-path not provided")]
    DbPathNotProvided,
    #[error("operation name not provided")]
    OperationNotProvided,
    #[error("item name not provided")]
    ItemNameNotProvided,
    #[error("operation {0} is not a valid operation")]
    InvalidOperation(String),
}

enum Operation {
    CreateItem { name: String },
    ListItems,
}

struct Args {
    db_path: PathBuf,
    operation: Operation,
}

impl Args {
    fn parse(mut it: impl Iterator<Item = String>) -> Result<Args, ArgParseError> {
        let _program_name = it.next();
        let db_path = it
            .next()
            .map(Into::into)
            .ok_or(ArgParseError::DbPathNotProvided)?;
        let operation_name = it.next().ok_or(ArgParseError::OperationNotProvided)?;

        let operation = match operation_name.as_ref() {
            "create_item" => {
                let name = it.next().ok_or(ArgParseError::ItemNameNotProvided)?;
                Operation::CreateItem { name }
            }
            "list_items" => Operation::ListItems,
            _ => {
                return Err(ArgParseError::InvalidOperation(operation_name));
            }
        };

        Ok(Args { db_path, operation })
    }
}

#[derive(Error)]
enum MainError {
    #[error("argument parsing failed")]
    ArgParse(#[source] ArgParseError),
    #[error("failed to open database")]
    OpenDb(#[source] todo_fs::db::OpenDbError),
    #[error("create item failed")]
    CreateItem(#[source] CreateItemError),
    #[error("failed to get items")]
    GetItems(#[source] todo_fs::db::QueryError),
}

// main will print the debug implementation, so use that as our user presentable view
impl fmt::Debug for MainError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut err: &dyn Error = self;

        writeln!(f, "{}\n", err)?;
        writeln!(f, "caused by: ")?;

        while let Some(source) = err.source() {
            err = source;
            writeln!(f, "{}", source)?;
        }

        Ok(())
    }
}

fn main() -> Result<(), MainError> {
    env_logger::init();

    let args = Args::parse(std::env::args()).map_err(MainError::ArgParse)?;
    let mut db = Db::new(args.db_path).map_err(MainError::OpenDb)?;

    match args.operation {
        Operation::CreateItem { name } => {
            db.create_item(&name).map_err(MainError::CreateItem)?;
        }
        Operation::ListItems => {
            for item in db.get_items().map_err(MainError::GetItems)? {
                println!("{:?}", item);
            }
        }
    }

    Ok(())
}
