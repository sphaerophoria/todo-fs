use std::{
    ffi::{OsStr, OsString},
    fs,
    path::{Path, PathBuf},
};

use crate::db::{Db, GetItemsError, ItemId};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ParsePathError {
    #[error("failed to check item folder match")]
    CheckItemFolderMatch(#[source] std::io::Error),
}

#[derive(Debug, Error)]
pub enum ReadDirError {
    #[error("failed to parse path")]
    ParsePath(#[source] ParsePathError),
    #[error("failed to get items")]
    GetItems(#[source] GetItemsError),
    #[error("failed to read db dir")]
    ReadDbDir(#[source] std::io::Error),
}

pub enum DirEntry {
    Dir(OsString),
    File(OsString),
}

#[derive(Debug)]
enum PathPurpose {
    Root,
    Items,
    Item(ItemId),
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

#[derive(Debug)]
pub struct FuseClient {
    pub db: Db,
}

impl FuseClient {
    pub fn get_passthrough_path(&self, path: &Path) -> Result<Option<PathBuf>, std::io::Error> {
        matches_item_content_folder(path, &self.db)
    }

    pub fn is_dir(&self, path: &Path) -> Result<bool, ParsePathError> {
        match self.parse_path(path)? {
            PathPurpose::Root
            | PathPurpose::Items
            | PathPurpose::Item(_)
            | PathPurpose::Unknown => Ok(true),
            PathPurpose::DbPath(_) => panic!("Db paths not handled by client"),
        }
    }

    pub fn readdir(
        &self,
        path: &Path,
    ) -> Result<Box<dyn Iterator<Item = DirEntry> + '_>, ReadDirError> {
        let ret: Box<dyn Iterator<Item = DirEntry> + '_> =
            match self.parse_path(path).map_err(ReadDirError::ParsePath)? {
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
                PathPurpose::Item(_) => Box::new([DirEntry::Dir("content".into())].into_iter()),
                PathPurpose::Unknown => {
                    log::warn!("Unhandled path: {path:?}");
                    Box::new([].into_iter())
                }
            };

        Ok(ret)
    }

    fn parse_path(&self, path: &Path) -> Result<PathPurpose, ParsePathError> {
        // /by-id/1 -> /some/path/to/db/1
        let ret = if path == Path::new("/") {
            PathPurpose::Root
        } else if path == Path::new(ITEMS_FOLDER) {
            PathPurpose::Items
        } else if let Some(db_path) = matches_item_content_folder(path, &self.db)
            .map_err(ParsePathError::CheckItemFolderMatch)?
        {
            PathPurpose::DbPath(db_path)
        } else if let Some(item_id) = matches_item_folder(path) {
            PathPurpose::Item(item_id)
        } else {
            println!("Unhandled path: {:?}", path);
            PathPurpose::Unknown
        };

        Ok(ret)
    }
}
