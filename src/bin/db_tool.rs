use std::{error::Error, fmt, path::PathBuf};
use thiserror::Error;
use todo_fs::db::{CreateItemError, Db, ItemId, RelationshipId};

extern crate todo_fs;

#[derive(Debug, Error)]
enum ArgParseError {
    #[error("db-path not provided")]
    DbPathNotProvided,
    #[error("operation name not provided")]
    OperationNotProvided,
    #[error("item name not provided")]
    ItemNameNotProvided,
    #[error("from name not provided")]
    FromNameNotProvided,
    #[error("to name not provided")]
    ToNameNotProvided,
    #[error("from id not provided")]
    FromIdNotProvided,
    #[error("to id not provided")]
    ToIdNotProvided,
    #[error("relationship id not provided")]
    RelationshipIdNotProvided,
    #[error("from id invalid")]
    InvalidFromId(#[source] std::num::ParseIntError),
    #[error("to id invalid")]
    InvalidToId(#[source] std::num::ParseIntError),
    #[error("relationship id invalid")]
    InvalidRelationshipId(#[source] std::num::ParseIntError),
    #[error("operation {0} is not a valid operation")]
    InvalidOperation(String),
}

enum Operation {
    CreateItem {
        name: String,
    },
    AddRelationship {
        from_name: String,
        to_name: String,
    },
    AddItemRelationship {
        from_id: i64,
        to_id: i64,
        relationship_id: i64,
    },
    ListRelationships,
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
            "add_relationship" => {
                let from_name = it.next().ok_or(ArgParseError::FromNameNotProvided)?;
                let to_name = it.next().ok_or(ArgParseError::ToNameNotProvided)?;
                Operation::AddRelationship { from_name, to_name }
            }
            "list_relationships" => Operation::ListRelationships,
            "add_item_relationship" => {
                let from_id = it
                    .next()
                    .ok_or(ArgParseError::FromIdNotProvided)?
                    .parse()
                    .map_err(ArgParseError::InvalidFromId)?;
                let to_id = it
                    .next()
                    .ok_or(ArgParseError::ToIdNotProvided)?
                    .parse()
                    .map_err(ArgParseError::InvalidToId)?;
                let relationship_id = it
                    .next()
                    .ok_or(ArgParseError::RelationshipIdNotProvided)?
                    .parse()
                    .map_err(ArgParseError::InvalidRelationshipId)?;
                Operation::AddItemRelationship {
                    from_id,
                    to_id,
                    relationship_id,
                }
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
    #[error("failed to add relationship")]
    AddRelationship(#[source] todo_fs::db::AddRelationshipError),
    #[error("failed to get relationships")]
    GetRelationships(#[source] todo_fs::db::QueryError),
    #[error("failed to add item relationship")]
    AddItemRelationship(#[source] todo_fs::db::AddItemRelationshipError),
    #[error("failed to get items")]
    GetItems(#[source] todo_fs::db::GetItemsError),
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
        Operation::AddRelationship { from_name, to_name } => {
            db.add_relationship(&from_name, &to_name)
                .map_err(MainError::AddRelationship)?;
        }
        Operation::ListRelationships => {
            for relationship in db
                .get_relationships()
                .map_err(MainError::GetRelationships)?
            {
                println!("{:?}", relationship);
            }
        }
        Operation::AddItemRelationship {
            from_id,
            to_id,
            relationship_id,
        } => db
            .add_item_relationship(
                ItemId(from_id),
                ItemId(to_id),
                RelationshipId(relationship_id),
            )
            .map_err(MainError::AddItemRelationship)?,
        Operation::ListItems => {
            for item in db.get_items().map_err(MainError::GetItems)? {
                println!("{:?}", item);
            }
        }
    }

    Ok(())
}
