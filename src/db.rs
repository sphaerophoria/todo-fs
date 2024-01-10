#![allow(unused)]

use rusqlite::Connection;
use std::{
    fs,
    path::{Path, PathBuf},
};
use thiserror::Error;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ItemId(pub i64);

#[derive(Debug, Error)]
pub enum CreateItemError {
    #[error("failed to start transaction")]
    StartTransaction(#[source] rusqlite::Error),
    #[error("item already exists")]
    ItemExists,
    #[error("failed to insert item into database")]
    InsertItem(#[source] rusqlite::Error),
    #[error("failed to create content folder")]
    CreateContentFolder(#[source] std::io::Error),
    #[error("failed to commit transaction")]
    CommitTransaction(#[source] rusqlite::Error),
}

#[derive(Debug, Error)]
pub enum OpenDbError {
    #[error("failed to create directory for content")]
    CreateFilesDir(#[source] std::io::Error),
    #[error("failed to open connection with db")]
    OpenConnection(#[source] rusqlite::Error),
    #[error("failed to start transaction")]
    StartTransaction(#[source] rusqlite::Error),
    #[error("failed to create files table")]
    CreateFilesTable(#[source] rusqlite::Error),
    #[error("failed to create relationships table")]
    CreateRelationshipsTable(#[source] rusqlite::Error),
    #[error("failed to create item relationships table")]
    CreateItemRelationshipsTable(#[source] rusqlite::Error),
    #[error("failed to commit transactions")]
    CommitTransaction(#[source] rusqlite::Error),
}

#[derive(Debug, Error)]
pub enum QueryError {
    #[error("failed to prepare query")]
    Prepare(#[source] rusqlite::Error),
    #[error("failed to execute query")]
    Execute(#[source] rusqlite::Error),
    #[error("failed to map results")]
    QueryMapFailed(#[source] rusqlite::Error),
}

#[derive(Debug)]
pub struct Db {
    item_path: PathBuf,
    connection: Connection,
}

#[derive(Debug)]
pub struct DbItem {
    pub path: PathBuf,
    pub id: ItemId,
    pub name: String,
}

impl Db {
    pub fn new(path: PathBuf) -> Result<Db, OpenDbError> {
        if !path.exists() {
            fs::create_dir_all(&path).map_err(OpenDbError::CreateFilesDir)?;
        }

        let sqlite_path = path.join("metadata.db");
        let mut connection = Connection::open(sqlite_path).map_err(OpenDbError::OpenConnection)?;
        let transaction = connection
            .transaction()
            .map_err(OpenDbError::StartTransaction)?;

        transaction
            .execute(
                "CREATE TABLE IF NOT EXISTS files(id INTEGER PRIMARY KEY, name TEXT NOT NULL)",
                (),
            )
            .map_err(OpenDbError::CreateFilesTable)?;

        transaction
            .execute(
                "CREATE TABLE IF NOT EXISTS relationships(id INTEGER PRIMARY KEY, from_name TEXT NOT NULL, to_name TEXT_NOT_NULL)",
                (),
            )
            .map_err(OpenDbError::CreateRelationshipsTable)?;

        transaction
            .execute(
                "CREATE TABLE IF NOT EXISTS item_relationships(from_id INTEGER, to_id INTEGER, relationship_id INTEGER,
                FOREIGN KEY(from_id) REFERENCES files(id),
                FOREIGN KEY(to_id) REFERENCES files(id),
                FOREIGN KEY(relationship_id) REFERENCES relationships(id),
                UNIQUE(from_id, to_id, relationship_id))",
                (),
            )
            .map_err(OpenDbError::CreateItemRelationshipsTable)?;

        transaction
            .commit()
            .map_err(OpenDbError::CommitTransaction)?;
        let item_path = path.join("items");
        Ok(Db {
            item_path,
            connection,
        })
    }

    pub fn create_item(&mut self, name: &str) -> Result<(), CreateItemError> {
        let transaction = self
            .connection
            .transaction()
            .map_err(CreateItemError::StartTransaction)?;
        transaction
            .execute("INSERT INTO files(name) VALUES (?1)", [name])
            .map_err(CreateItemError::InsertItem);
        let id = transaction.last_insert_rowid();

        let item_path = self.item_path.join(id.to_string());
        if item_path.exists() {
            return Err(CreateItemError::ItemExists);
        }

        fs::create_dir_all(item_path).map_err(CreateItemError::CreateContentFolder);

        transaction
            .commit()
            .map_err(CreateItemError::CommitTransaction)?;
        Ok(())
    }

    pub fn fs_root(&self) -> &Path {
        &self.item_path
    }

    pub fn content_folder_for_id(&self, id: ItemId) -> Result<PathBuf, std::io::Error> {
        self.item_path.join(id.0.to_string()).canonicalize()
    }

    pub fn get_item_by_id(&self, id: ItemId) -> Option<DbItem> {
        // FIXME: Don't query the whole database for every item lookup idiot
        self.get_items()
            .into_iter()
            .flatten()
            .find(|item| item.id == id)
    }

    pub fn get_items(&self) -> Result<Vec<DbItem>, QueryError> {
        // files(id, name)
        // item_relationships(from_id, to_id, relationship_id)
        let mut statement = self
            .connection
            .prepare("SELECT id, name FROM files")
            .map_err(QueryError::Prepare)?;

        let items: Vec<DbItem> = statement
            .query_map([], |row| {
                let id: i64 = row.get(0)?;
                let id = ItemId(id);
                Ok(DbItem {
                    path: self.content_folder_for_id(id).unwrap(),
                    id,
                    name: row.get(1)?,
                })
            })
            .map_err(QueryError::Execute)?
            .map(|x| x.map_err(QueryError::QueryMapFailed))
            .collect::<Result<Vec<DbItem>, QueryError>>()?;

        Ok(items)
    }
}
