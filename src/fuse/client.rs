use std::{
    collections::HashSet,
    ffi::OsString,
    fs,
    path::{Path, PathBuf},
};

use crate::db::{Db, GetItemsError, ItemId, ItemRelationship, RelationshipId, RelationshipSide};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum CategorizeRelationshipsError {
    #[error("failed to get relationships")]
    GetRelationshipsFailed(#[source] crate::db::QueryError),
    #[error("relationship {0} does not exist")]
    RelationshipNonExistent(i64),
}

#[derive(Debug, Error)]
pub enum ParsePathError {
    #[error("failed to list parent dir")]
    ReadDir(#[from] ReadDirError),
    #[error("failed to parse path name")]
    ParsePath,
}

#[derive(Debug, Error)]
pub enum ReadDirError {
    #[error("failed to parse path")]
    ParsePath(#[source] Box<ParsePathError>),
    #[error("failed to get items")]
    GetItems(#[source] GetItemsError),
    #[error("failed to read db dir")]
    ReadDbDir(#[source] std::io::Error),
    #[error("item id not in db")]
    ItemIdNotInDatabase,
    #[error("failed to categorize relationships")]
    CategorizeRelationships(#[source] CategorizeRelationshipsError),
    #[error("failed to get content folder for item")]
    GetContentFolder(#[source] std::io::Error),
    #[error("failed to get filetype for path")]
    GetFiletype(#[source] std::io::Error),
    #[error("read dir called on non directory")]
    NotADirectory,
}

#[derive(Debug, Error)]
pub enum GetFiletypeError {
    #[error("failed to parse path")]
    ParsePath(#[source] ParsePathError),
    #[error("failed to get file type for file")]
    GetFileType(#[source] std::io::Error),
}

#[derive(Debug, Error)]
pub enum ReadLinkError {
    #[error("failed to parse path")]
    ParsePath(#[source] ParsePathError),
    #[error("item is not a link")]
    NotALink,
}

fn categorize_relationships(
    relationships: &Vec<ItemRelationship>,
    db: &Db,
) -> Result<Vec<(RelationshipId, RelationshipSide, String)>, CategorizeRelationshipsError> {
    let mut ret = HashSet::new();

    for item_relationship in relationships {
        let relationship = db
            .get_relationship(item_relationship.id)
            .map_err(CategorizeRelationshipsError::GetRelationshipsFailed)?
            .ok_or_else(|| {
                CategorizeRelationshipsError::RelationshipNonExistent(item_relationship.id.0)
            })?;

        let name = match item_relationship.side {
            RelationshipSide::Dest => relationship.from_name,
            RelationshipSide::Source => relationship.to_name,
        };

        ret.insert((item_relationship.id, item_relationship.side, name));
    }

    Ok(ret.into_iter().collect())
}

pub enum DirEntry {
    Dir(OsString),
    File(OsString),
    Link(OsString),
}

pub enum Filetype {
    Dir,
    File,
    Link,
}

#[derive(Debug)]
enum PathPurpose {
    Root,
    Items,
    Item(ItemId),
    ItemRelationships(ItemId, RelationshipId, RelationshipSide),
    ItemLink(ItemId),
    DbPath(PathBuf),
    Unknown,
}

const ITEMS_FOLDER: &str = "/items";

fn path_purpose_to_filetype(purpose: &PathPurpose) -> Result<Filetype, std::io::Error> {
    let ret = match purpose {
        PathPurpose::Root
        | PathPurpose::Items
        | PathPurpose::Item(_)
        | PathPurpose::ItemRelationships(_, _, _)
        | PathPurpose::Unknown => Filetype::Dir,
        PathPurpose::ItemLink(_) => Filetype::Link,
        PathPurpose::DbPath(p) => {
            println!("{:?}", p);
            let metadata = p.metadata()?;
            if metadata.is_dir() {
                Filetype::Dir
            } else if metadata.is_symlink() {
                Filetype::Link
            } else {
                Filetype::File
            }
        }
    };

    Ok(ret)
}

#[derive(Debug)]
pub struct FuseClient {
    pub db: Db,
}

impl FuseClient {
    pub fn get_passthrough_path(&mut self, path: &Path) -> Result<Option<PathBuf>, ParsePathError> {
        if let PathPurpose::DbPath(p) = self.parse_path(path)? {
            return Ok(Some(p));
        }

        Ok(None)
    }

    pub fn get_filetype(&mut self, path: &Path) -> Result<Filetype, GetFiletypeError> {
        path_purpose_to_filetype(&self.parse_path(path).map_err(GetFiletypeError::ParsePath)?)
            .map_err(GetFiletypeError::GetFileType)
    }

    fn list_dir_contents(
        &mut self,
        path: &Path,
    ) -> Result<Box<dyn Iterator<Item = (PathPurpose, String)> + '_>, ReadDirError> {
        let ret: Box<dyn Iterator<Item = (PathPurpose, String)> + '_> = match self
            .parse_path(path)
            .map_err(|x| ReadDirError::ParsePath(Box::new(x)))?
        {
            PathPurpose::Root => {
                let items_iter = [(PathPurpose::Items, "items".to_string())].into_iter();

                Box::new(items_iter)
            }
            PathPurpose::Items => Box::new(
                self.db
                    .get_items()
                    .map_err(ReadDirError::GetItems)?
                    .into_iter()
                    .map(|item| (PathPurpose::Item(item.id), item.id.0.to_string())),
            ),
            PathPurpose::Item(id) => {
                let item = self
                    .db
                    .get_item_by_id(id)
                    .ok_or(ReadDirError::ItemIdNotInDatabase)?;
                let relationships = categorize_relationships(&item.relationships, &self.db)
                    .map_err(ReadDirError::CategorizeRelationships)?;
                let db_path = self
                    .db
                    .content_folder_for_id(id)
                    .map_err(ReadDirError::GetContentFolder)?;
                let names = relationships.into_iter().map(
                    move |(relationship_id, relationship_side, name)| {
                        (
                            PathPurpose::ItemRelationships(id, relationship_id, relationship_side),
                            name,
                        )
                    },
                );
                Box::new(names.chain([(PathPurpose::DbPath(db_path), "content".to_string())]))
            }
            PathPurpose::ItemLink(_) => return Err(ReadDirError::NotADirectory),
            PathPurpose::ItemRelationships(item_id, relationship_id, relationship_side) => {
                let item = self
                    .db
                    .get_item_by_id(item_id)
                    .ok_or(ReadDirError::ItemIdNotInDatabase)?;

                let item_relationships =
                    item.relationships.into_iter().filter(move |relationship| {
                        relationship.id == relationship_id && relationship.side == relationship_side
                    });

                let it = item_relationships.map(
                    |item_relationship| -> Result<(PathPurpose, String), ItemId> {
                        let sibling = self
                            .db
                            .get_item_by_id(item_relationship.sibling)
                            .ok_or(item_relationship.sibling)?;
                        Ok((PathPurpose::ItemLink(sibling.id), sibling.name))
                    },
                );

                let it = it.filter_map(|item| match item {
                    Ok(v) => Some(v),
                    Err(id) => {
                        log::error!("item {} not present in db", id.0);
                        None
                    }
                });

                Box::new(it)
            }
            PathPurpose::DbPath(p) => {
                let it = fs::read_dir(p).map_err(ReadDirError::ReadDbDir)?.map(
                    |item| -> Result<(PathPurpose, String), String> {
                        let item = item.map_err(|e| e.to_string())?;
                        Ok((
                            PathPurpose::DbPath(item.path()),
                            item.file_name()
                                .to_str()
                                .ok_or_else(|| "failed to turn file name into string".to_string())?
                                .to_string(),
                        ))
                    },
                );

                let it = it.filter_map(|item| match item {
                    Ok(v) => Some(v),
                    Err(e) => {
                        log::error!("Failed to read item in dir: {e}");
                        None
                    }
                });

                Box::new(it)
            }
            PathPurpose::Unknown => {
                log::warn!("Unhandled path: {path:?}");
                return Err(ReadDirError::NotADirectory);
            }
        };

        Ok(ret)
    }

    pub fn readdir(
        &mut self,
        path: &Path,
    ) -> Result<impl Iterator<Item = DirEntry> + '_, ReadDirError> {
        let dir_it = self.list_dir_contents(path)?;
        let dir_it = dir_it.map(|item| {
            let ret = match path_purpose_to_filetype(&item.0).map_err(ReadDirError::GetFiletype)? {
                Filetype::Dir => DirEntry::Dir(item.1.into()),
                Filetype::Link => DirEntry::Link(item.1.into()),
                Filetype::File => DirEntry::File(item.1.into()),
            };
            Ok(ret)
        });

        let dir_it = dir_it.collect::<Result<Vec<_>, _>>()?.into_iter();
        Ok(dir_it)
    }

    pub fn readlink(&mut self, path: &Path) -> Result<PathBuf, ReadLinkError> {
        let item_id = match self.parse_path(path).map_err(ReadLinkError::ParsePath)? {
            PathPurpose::ItemLink(item_id) => item_id,
            _ => return Err(ReadLinkError::NotALink),
        };

        let mut output_path = PathBuf::new();
        let num_components = path.iter().count() - 2;
        for _ in 0..num_components {
            output_path.push("..")
        }
        output_path.push(&ITEMS_FOLDER[1..]);
        output_path.push(item_id.0.to_string());
        Ok(output_path)
    }

    fn parse_path(&mut self, path: &Path) -> Result<PathPurpose, ParsePathError> {
        let Some(parent) = path.parent() else {
            return Ok(PathPurpose::Root);
        };

        let Some(name) = path.file_name() else {
            return Ok(PathPurpose::Unknown);
        };

        let name = name.to_str().ok_or(ParsePathError::ParsePath)?;

        let Some(item) = self.list_dir_contents(parent)?.find(|item| item.1 == name) else {
            return Ok(PathPurpose::Unknown);
        };

        Ok(item.0)
    }
}
