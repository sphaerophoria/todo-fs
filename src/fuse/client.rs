use std::{
    collections::HashMap,
    ffi::{OsStr, OsString},
    fs,
    path::{Path, PathBuf},
};

use crate::db::{
    Db, GetItemsError, ItemId, ItemRelationship, QueryError, RelationshipId, RelationshipSide,
};
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
    #[error("failed to check item folder match")]
    Folder(#[source] std::io::Error),
    #[error("failed to check item relationship folder match")]
    Relationship(#[source] QueryError),
    #[error("failed to check if item link match")]
    Link(#[source] QueryError),
}

#[derive(Debug, Error)]
pub enum ReadDirError {
    #[error("failed to parse path")]
    ParsePath(#[source] ParsePathError),
    #[error("failed to get items")]
    GetItems(#[source] GetItemsError),
    #[error("failed to read db dir")]
    ReadDbDir(#[source] std::io::Error),
    #[error("item id not in db")]
    ItemIdNotInDatabase,
    #[error("failed to categorize relationships")]
    CategorizeRelationships(#[source] CategorizeRelationshipsError),
    #[error("read dir called on non directory")]
    NotADirectory,
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
) -> Result<HashMap<String, Vec<ItemId>>, CategorizeRelationshipsError> {
    let mut ret = HashMap::new();

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

        let name_items = ret.entry(name).or_insert(Vec::new());
        name_items.push(item_relationship.sibling);
    }

    Ok(ret)
}

pub enum DirEntry {
    Dir(OsString),
    File(OsString),
    Link(OsString),
}

pub enum Filetype {
    Dir,
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

fn has_items_parent<'a>(it: &mut impl Iterator<Item = &'a OsStr>) -> bool {
    Path::new(ITEMS_FOLDER).iter().eq(it.take(2))
}

fn matches_item_folder(path: &Path) -> Option<ItemId> {
    let mut it = path.iter();
    if !has_items_parent(&mut it) {
        return None;
    }

    let id = it.next();
    let content = it.next();

    if content.is_none() {
        let id = id.expect("id should exist if content exists");
        let id = id.to_str()?;
        let id = id.parse::<i64>().ok()?;
        return Some(ItemId(id));
    }

    None
}

fn matches_item_content_folder(path: &Path, db: &Db) -> Result<Option<PathBuf>, std::io::Error> {
    let mut it = path.iter();
    if !has_items_parent(&mut it) {
        return Ok(None);
    }

    let id = it.next();
    let content = it.next();
    let remaining: PathBuf = it.collect();

    if content == Some(OsStr::new("content")) {
        let id = id.expect("id should exist if content exists");
        let Some(id) = id.to_str() else {
            return Ok(None);
        };
        let Some(id) = id.parse::<i64>().ok() else {
            return Ok(None);
        };
        let id = ItemId(id);
        let full_path = db.content_folder_for_id(id)?.join(remaining);
        return Ok(Some(full_path));
    }

    Ok(None)
}

fn matches_item_relationship_folder(
    path: &Path,
    db: &Db,
) -> Result<Option<(ItemId, RelationshipId, RelationshipSide)>, QueryError> {
    let mut it = path.iter();
    if !has_items_parent(&mut it) {
        return Ok(None);
    }

    let item_id = it.next();
    // Relationship folder comes from third, if none it cannot match
    let Some(third) = it.next() else {
        return Ok(None);
    };
    let fourth = it.next();

    // If we're in a subfolder we are not in the exact folder we want
    if fourth.is_some() {
        return Ok(None);
    }

    let relationships = db.get_relationships()?;
    let Some(third) = third.to_str() else {
        return Ok(None);
    };

    let Some(relationship) = relationships
        .iter()
        .find(|relationship| relationship.from_name == third || relationship.to_name == third)
    else {
        return Ok(None);
    };

    let side = if relationship.from_name == third {
        RelationshipSide::Dest
    } else {
        RelationshipSide::Source
    };

    let item_id = item_id.expect("Item id should always be valid if relationship folder resolved");
    let Some(item_id) = item_id.to_str() else {
        return Ok(None);
    };
    let Ok(item_id) = item_id.parse() else {
        return Ok(None);
    };

    Ok(Some((ItemId(item_id), relationship.id, side)))
}

fn matches_item_link(path: &Path, db: &Db) -> Result<Option<ItemId>, QueryError> {
    let Some(parent) = path.parent() else {
        return Ok(None);
    };

    let Some(relationship_folder) = matches_item_relationship_folder(parent, db)? else {
        return Ok(None);
    };

    let sibling_name = path.file_name().unwrap().to_str().unwrap();
    db.get_sibling_id(
        relationship_folder.0,
        relationship_folder.2,
        relationship_folder.1,
        sibling_name,
    )
}

#[derive(Debug)]
pub struct FuseClient {
    pub db: Db,
}

impl FuseClient {
    pub fn get_passthrough_path(&self, path: &Path) -> Result<Option<PathBuf>, std::io::Error> {
        matches_item_content_folder(path, &self.db)
    }

    pub fn get_filetype(&self, path: &Path) -> Result<Filetype, ParsePathError> {
        match self.parse_path(path)? {
            PathPurpose::Root
            | PathPurpose::Items
            | PathPurpose::Item(_)
            | PathPurpose::ItemRelationships(_, _, _)
            | PathPurpose::Unknown => Ok(Filetype::Dir),
            PathPurpose::ItemLink(_) => Ok(Filetype::Link),
            PathPurpose::DbPath(_) => panic!("Db paths not handled by client"),
        }
    }

    pub fn readdir(
        &self,
        path: &Path,
    ) -> Result<Box<dyn Iterator<Item = DirEntry> + '_>, ReadDirError> {
        let ret: Box<dyn Iterator<Item = DirEntry> + '_> = match self
            .parse_path(path)
            .map_err(ReadDirError::ParsePath)?
        {
            PathPurpose::Root => {
                let item_folder_name = Path::new(ITEMS_FOLDER)
                    .file_name()
                    .expect("Item folder name should be valid")
                    .to_os_string();
                Box::new([DirEntry::Dir(item_folder_name)].into_iter())
            }
            PathPurpose::Items => Box::new(
                self.db
                    .get_items()
                    .map_err(ReadDirError::GetItems)?
                    .into_iter()
                    .map(|item| DirEntry::Dir(item.id.0.to_string().into())),
            ),
            PathPurpose::DbPath(p) => {
                let it = fs::read_dir(p).map_err(ReadDirError::ReadDbDir)?.map(
                    |item| -> Result<DirEntry, std::io::Error> {
                        let item = item?;
                        let ft = item.file_type()?;
                        if ft.is_dir() {
                            Ok(DirEntry::Dir(item.file_name()))
                        } else {
                            Ok(DirEntry::File(item.file_name()))
                        }
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
            PathPurpose::Item(id) => {
                let item = self
                    .db
                    .get_item_by_id(id)
                    .ok_or(ReadDirError::ItemIdNotInDatabase)?;
                let relationships = categorize_relationships(&item.relationships, &self.db)
                    .map_err(ReadDirError::CategorizeRelationships)?;
                let names = relationships
                    .into_keys()
                    .map(|name| DirEntry::Dir(name.into()));
                Box::new(names.chain([DirEntry::Dir("content".into())]))
            }
            PathPurpose::ItemRelationships(item_id, relationship_id, relationship_side) => {
                let item = self
                    .db
                    .get_item_by_id(item_id)
                    .ok_or(ReadDirError::ItemIdNotInDatabase)?;
                let item_relationships =
                    item.relationships.into_iter().filter(move |relationship| {
                        relationship.id == relationship_id && relationship.side == relationship_side
                    });

                let it = item_relationships.map(|item_relationship| -> Result<DirEntry, ItemId> {
                    let sibling = self
                        .db
                        .get_item_by_id(item_relationship.sibling)
                        .ok_or(item_relationship.sibling)?;
                    Ok(DirEntry::Link(sibling.name.into()))
                });

                let it = it.filter_map(|item| match item {
                    Ok(v) => Some(v),
                    Err(id) => {
                        log::error!("item {} not present in db", id.0);
                        None
                    }
                });

                Box::new(it)
            }
            PathPurpose::ItemLink(_) => {
                return Err(ReadDirError::NotADirectory);
            }
            PathPurpose::Unknown => {
                log::warn!("Unhandled path: {path:?}");
                Box::new([].into_iter())
            }
        };

        Ok(ret)
    }

    pub fn readlink(&self, path: &Path) -> Result<PathBuf, ReadLinkError> {
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

    fn parse_path(&self, path: &Path) -> Result<PathPurpose, ParsePathError> {
        let ret = if path == Path::new("/") {
            PathPurpose::Root
        } else if path == Path::new(ITEMS_FOLDER) {
            PathPurpose::Items
        } else if let Some(db_path) =
            matches_item_content_folder(path, &self.db).map_err(ParsePathError::Folder)?
        {
            PathPurpose::DbPath(db_path)
        } else if let Some(item_id) = matches_item_folder(path) {
            PathPurpose::Item(item_id)
        } else if let Some((item_id, relationship_id, relationship_side)) =
            matches_item_relationship_folder(path, &self.db)
                .map_err(ParsePathError::Relationship)?
        {
            PathPurpose::ItemRelationships(item_id, relationship_id, relationship_side)
        } else if let Some(item_id) =
            matches_item_link(path, &self.db).map_err(ParsePathError::Link)?
        {
            PathPurpose::ItemLink(item_id)
        } else {
            println!("Unhandled path: {:?}", path);
            PathPurpose::Unknown
        };

        Ok(ret)
    }
}
