use std::{
    collections::{HashMap, HashSet, VecDeque},
    ffi::OsString,
    fs,
    io::Read,
    path::{Path, PathBuf},
};

use crate::db::{
    Db, FilterId, GetItemsError, ItemId, ItemRelationship, QueryError, RelationshipId,
    RelationshipSide,
};
use thiserror::Error;

use super::api::{ClientRequest, ClientResponse, CreateItemResponse, CreateRelationshipResponse};

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
    #[error("failed to get relationships")]
    GetRelationships(#[source] crate::db::QueryError),
    #[error("failed to read db dir")]
    ReadDbDir(#[source] std::io::Error),
    #[error("item id not in db")]
    ItemIdNotInDatabase,
    #[error("failed to categorize relationships")]
    CategorizeRelationships(#[source] CategorizeRelationshipsError),
    #[error("failed to get filters from db")]
    GetFilters(#[source] crate::db::GetFiltersError),
    #[error("failed to find filter for given ID")]
    FindFilter,
    #[error("failed to run filter")]
    RunFilter(#[source] crate::db::QueryError),
    #[error("failed to get content folder for item")]
    GetContentFolder(#[source] std::io::Error),
    #[error("failed to get filetype for path")]
    GetFiletype(#[source] PathPurposeToFiletypeError),
    #[error("read dir called on non directory")]
    NotADirectory,
}

#[derive(Debug, Error)]
pub enum GetFiletypeError {
    #[error("failed to parse path")]
    ParsePath(#[source] ParsePathError),
    #[error("failed to get file type for file")]
    GetFileType(#[source] PathPurposeToFiletypeError),
}

#[derive(Debug, Error)]
pub enum ReadLinkError {
    #[error("failed to parse path")]
    ParsePath(#[source] ParsePathError),
    #[error("item is not a link")]
    NotALink,
}

#[derive(Debug, Error)]
pub enum WriteError {
    #[error("failed to parse json request")]
    ParseJson(#[source] serde_json::Error),
    #[error("failed to create item")]
    CreateItem(#[source] crate::db::CreateItemError),
    #[error("failed to create relationship")]
    CreateRelationship(#[from] crate::db::AddRelationshipError),
    #[error("failed to create item relationship")]
    CreateItemRelationship(#[from] crate::db::AddItemRelationshipError),
    #[error("failed to add filter")]
    AddFilter(#[from] crate::db::AddFilterError),
    #[error("failed to find response handle")]
    FindResponseHandle,
    #[error("failed to serialise response")]
    SerializeResponse(#[source] serde_json::Error),
}

#[derive(Debug, Error)]
pub enum ReadError {
    #[error("failed to find response handle")]
    FindResponseHandle,
    #[error("failed to read from output buffer")]
    Read(#[source] std::io::Error),
    #[error("unhandled path")]
    UnhandledPath,
    #[error("failed to parse path")]
    ParsePath(#[from] ParsePathError),
    #[error("failed to get from_name for relationship")]
    RelationshipFromName(#[source] QueryError),
    #[error("failed to get to_name for relationship")]
    RelationshipToName(#[source] QueryError),
}

#[derive(Debug, Error)]
pub enum PathPurposeToFiletypeError {
    #[error("failed to get metadata for passthrough file")]
    GetMetadata(#[source] std::io::Error),
    #[error("failed to get from_name for relationship")]
    RelationshipFromName(#[source] QueryError),
    #[error("failed to get to_name for relationship")]
    RelationshipToName(#[source] QueryError),
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
    File(usize),
    Link,
}

pub enum OpenRet {
    Socket(u64),
    Noop,
    Unhandled,
}

#[derive(Debug)]
enum PathPurpose {
    // root directory of entire filesystem
    Root,
    // directory where we store exectuables for interacting with fs
    ToolBins,
    // listing of all items by id
    Items,
    // listing of all relationships by id
    Relationships,
    // "socket" file that allows sending/receiving messages out of band to the fuse filesystem
    Socket,
    // Directory associated with a given itemid
    Item(ItemId),
    // metadata file that shows id of current item
    ItemId(ItemId),
    // metadata file that shows name of current item
    ItemName(ItemId),
    // Directory associated with a given relationship
    Relationship(RelationshipId),
    RelationshipId(RelationshipId),
    RelationshipFromName(RelationshipId),
    RelationshipToName(RelationshipId),
    // Folder showing all items associated with ItemId by relationship RelationshipId
    // e.g. in a parents <-> children relationship, this is a "parents" or "children" directory
    ItemRelationships(ItemId, RelationshipId, RelationshipSide),
    // A link to a specific item by id (presented by name)
    ItemLink(ItemId),
    // a path that is passed through to the real filesystem
    PassthroughPath(PathBuf),
    // Named filter that shows items filtered in some way
    Filter(FilterId),
    // Unknown
    Unknown,
}

const ITEMS_FOLDER: &str = "/items";
const RELATIONSHIPS_FOLDER: &str = "/relationships";

fn with_newline_as_vec(mut s: String) -> Vec<u8> {
    s += "\n";
    s.into_bytes()
}

fn get_item_id_file_contents(id: &ItemId) -> Vec<u8> {
    with_newline_as_vec(id.0.to_string())
}

fn get_relationship_id_file_contents(id: &RelationshipId) -> Vec<u8> {
    with_newline_as_vec(id.0.to_string())
}

fn get_item_name_file_contents(id: &ItemId, db: &Db) -> Vec<u8> {
    let Some(item) = db.get_item_by_id(*id) else {
        return Default::default();
    };
    with_newline_as_vec(item.name)
}

fn get_relationship_from_name_file_contents(
    id: &RelationshipId,
    db: &Db,
) -> Result<Vec<u8>, QueryError> {
    let Some(relationship) = db.get_relationship(*id)? else {
        return Ok(Default::default());
    };
    Ok(with_newline_as_vec(relationship.from_name))
}

fn get_relationship_to_name_file_contents(
    id: &RelationshipId,
    db: &Db,
) -> Result<Vec<u8>, QueryError> {
    let Some(relationship) = db.get_relationship(*id)? else {
        return Ok(Default::default());
    };
    Ok(with_newline_as_vec(relationship.to_name))
}

fn path_purpose_to_filetype(
    purpose: &PathPurpose,
    db: &Db,
) -> Result<Filetype, PathPurposeToFiletypeError> {
    let ret = match purpose {
        PathPurpose::Root
        | PathPurpose::ToolBins
        | PathPurpose::Items
        | PathPurpose::Relationships
        | PathPurpose::Item(_)
        | PathPurpose::Relationship(_)
        | PathPurpose::Filter(_)
        | PathPurpose::ItemRelationships(_, _, _)
        | PathPurpose::Unknown => Filetype::Dir,
        PathPurpose::ItemLink(_) => Filetype::Link,
        PathPurpose::Socket => Filetype::File(0),
        PathPurpose::ItemId(id) => {
            let content_length = get_item_id_file_contents(id).len();
            Filetype::File(content_length)
        }
        PathPurpose::ItemName(id) => {
            let content_length = get_item_name_file_contents(id, db).len();
            Filetype::File(content_length)
        }
        PathPurpose::RelationshipId(id) => {
            let content_length = get_relationship_id_file_contents(id).len();
            Filetype::File(content_length)
        }
        PathPurpose::RelationshipFromName(id) => {
            let content_length = get_relationship_from_name_file_contents(id, db)
                .map_err(PathPurposeToFiletypeError::RelationshipFromName)?
                .len();
            Filetype::File(content_length)
        }
        PathPurpose::RelationshipToName(id) => {
            let content_length = get_relationship_to_name_file_contents(id, db)
                .map_err(PathPurposeToFiletypeError::RelationshipToName)?
                .len();
            Filetype::File(content_length)
        }
        PathPurpose::PassthroughPath(p) => {
            let metadata = p
                .metadata()
                .map_err(PathPurposeToFiletypeError::GetMetadata)?;
            if metadata.is_dir() {
                Filetype::Dir
            } else if metadata.is_symlink() {
                Filetype::Link
            } else {
                Filetype::File(0)
            }
        }
    };

    Ok(ret)
}

#[derive(Debug)]
pub struct FuseClient {
    pub db: Db,
    latest_open_id: u64,
    open_files: HashMap<u64, VecDeque<u8>>,
}

impl FuseClient {
    pub fn new(db: Db) -> FuseClient {
        FuseClient {
            db,
            latest_open_id: 0,
            open_files: HashMap::new(),
        }
    }

    pub fn get_passthrough_path(&mut self, path: &Path) -> Result<Option<PathBuf>, ParsePathError> {
        if let PathPurpose::PassthroughPath(p) = self.parse_path(path)? {
            return Ok(Some(p));
        }

        Ok(None)
    }

    pub fn get_filetype(&mut self, path: &Path) -> Result<Filetype, GetFiletypeError> {
        path_purpose_to_filetype(
            &self.parse_path(path).map_err(GetFiletypeError::ParsePath)?,
            &self.db,
        )
        .map_err(GetFiletypeError::GetFileType)
    }

    pub fn open(&mut self, path: &Path) -> Result<OpenRet, ParsePathError> {
        match self.parse_path(path)? {
            PathPurpose::Socket => (),
            PathPurpose::ItemId(_)
            | PathPurpose::ItemName(_)
            | PathPurpose::RelationshipId(_)
            | PathPurpose::RelationshipToName(_)
            | PathPurpose::RelationshipFromName(_) => {
                return Ok(OpenRet::Noop);
            }
            _ => return Ok(OpenRet::Unhandled),
        };

        self.open_files.insert(self.latest_open_id, VecDeque::new());
        let id = self.latest_open_id;
        self.latest_open_id += 1;

        Ok(OpenRet::Socket(id))
    }

    pub fn write(&mut self, id: u64, buf: &[u8]) -> Result<(), WriteError> {
        let req = serde_json::from_slice::<ClientRequest>(buf).map_err(WriteError::ParseJson)?;

        match req {
            ClientRequest::CreateItem(create_item_req) => {
                let item_id = self
                    .db
                    .create_item(&create_item_req.name)
                    .map_err(WriteError::CreateItem)?;
                let new_item_path = Path::new(ITEMS_FOLDER).join(item_id.0.to_string());
                let response = CreateItemResponse {
                    path: new_item_path,
                };

                let response = ClientResponse::CreateItem(response);

                let response_file = self
                    .open_files
                    .get_mut(&id)
                    .ok_or(WriteError::FindResponseHandle)?;
                serde_json::to_writer(response_file, &response)
                    .map_err(WriteError::SerializeResponse)?;
            }
            ClientRequest::CreateRelationship(req) => {
                let item_id = self.db.add_relationship(&req.from_name, &req.to_name)?;
                let new_item_path = Path::new(RELATIONSHIPS_FOLDER).join(item_id.0.to_string());

                let response = CreateRelationshipResponse {
                    path: new_item_path,
                };
                let response = ClientResponse::CreateRelationship(response);

                let response_file = self
                    .open_files
                    .get_mut(&id)
                    .ok_or(WriteError::FindResponseHandle)?;
                serde_json::to_writer(response_file, &response)
                    .map_err(WriteError::SerializeResponse)?;
            }
            ClientRequest::CreateFilter(req) => {
                self.db.add_filter(&req.name, &req.filters)?;
            }
            ClientRequest::CreateItemRelationship(req) => {
                println!("Adding item relationship");
                self.db.add_item_relationship(
                    ItemId(req.from_id),
                    ItemId(req.to_id),
                    RelationshipId(req.relationship_id),
                )?;
            }
        }

        Ok(())
    }

    pub fn read(&mut self, path: &Path, id: u64, buf: &mut [u8]) -> Result<usize, ReadError> {
        let parsed_path = self.parse_path(path)?;
        match parsed_path {
            PathPurpose::Socket => {
                let f = self
                    .open_files
                    .get_mut(&id)
                    .ok_or(ReadError::FindResponseHandle)?;
                f.read(buf).map_err(ReadError::Read)
            }
            PathPurpose::ItemId(id) => {
                let content = get_item_id_file_contents(&id);
                buf[0..content.len()].copy_from_slice(&content);
                Ok(content.len())
            }
            PathPurpose::ItemName(id) => {
                let content = get_item_name_file_contents(&id, &self.db);
                buf[0..content.len()].copy_from_slice(&content);
                Ok(content.len())
            }
            PathPurpose::RelationshipId(id) => {
                let content = get_relationship_id_file_contents(&id);
                buf[0..content.len()].copy_from_slice(&content);
                Ok(content.len())
            }
            PathPurpose::RelationshipFromName(id) => {
                let content = get_relationship_from_name_file_contents(&id, &self.db)
                    .map_err(ReadError::RelationshipFromName)?;
                buf[0..content.len()].copy_from_slice(&content);
                Ok(content.len())
            }
            PathPurpose::RelationshipToName(id) => {
                let content = get_relationship_to_name_file_contents(&id, &self.db)
                    .map_err(ReadError::RelationshipToName)?;
                buf[0..content.len()].copy_from_slice(&content);
                Ok(content.len())
            }
            _ => Err(ReadError::UnhandledPath),
        }
    }

    pub fn release(&mut self, id: u64) {
        self.open_files.remove(&id);
    }

    fn list_dir_contents(
        &mut self,
        path: PathPurpose,
    ) -> Result<Box<dyn Iterator<Item = (PathPurpose, String)> + '_>, ReadDirError> {
        let ret: Box<dyn Iterator<Item = (PathPurpose, String)> + '_> = match path {
            PathPurpose::Root => {
                let items_iter = [
                    (PathPurpose::Items, ITEMS_FOLDER[1..].to_string()),
                    (
                        PathPurpose::Relationships,
                        RELATIONSHIPS_FOLDER[1..].to_string(),
                    ),
                    (PathPurpose::ToolBins, "bin".to_string()),
                    (
                        PathPurpose::Socket,
                        crate::fuse::api::API_HANDLE_PATH[1..].to_string(),
                    ),
                ]
                .into_iter();

                let filters_iter = self
                    .db
                    .get_filters()
                    .map_err(ReadDirError::GetFilters)?
                    .into_iter()
                    .map(|filter| (PathPurpose::Filter(filter.id), filter.name));

                Box::new(items_iter.chain(filters_iter))
            }
            PathPurpose::Items => Box::new(
                self.db
                    .get_items()
                    .map_err(ReadDirError::GetItems)?
                    .into_iter()
                    .map(|item| (PathPurpose::Item(item.id), item.id.0.to_string())),
            ),
            PathPurpose::Relationships => Box::new(
                self.db
                    .get_relationships()
                    .map_err(ReadDirError::GetRelationships)?
                    .into_iter()
                    .map(|relationship| {
                        (
                            PathPurpose::Relationship(relationship.id),
                            relationship.id.0.to_string(),
                        )
                    }),
            ),
            PathPurpose::Relationship(id) => Box::new(
                [
                    (PathPurpose::RelationshipId(id), "id".to_string()),
                    (
                        PathPurpose::RelationshipFromName(id),
                        "from_name".to_string(),
                    ),
                    (PathPurpose::RelationshipToName(id), "to_name".to_string()),
                ]
                .into_iter(),
            ),
            PathPurpose::Item(id) => {
                let item = self
                    .db
                    .get_item_by_id(id)
                    .ok_or(ReadDirError::ItemIdNotInDatabase)?;
                let relationships = categorize_relationships(&item.relationships, &self.db)
                    .map_err(ReadDirError::CategorizeRelationships)?;
                let passthrough_path = self
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

                Box::new(names.chain([
                    (
                        PathPurpose::PassthroughPath(passthrough_path),
                        "content".to_string(),
                    ),
                    (PathPurpose::ItemId(id), "id".to_string()),
                    (PathPurpose::ItemName(id), "name".to_string()),
                ]))
            }
            PathPurpose::Filter(filter_id) => {
                let filter = self
                    .db
                    .get_filters()
                    .map_err(ReadDirError::GetFilters)?
                    .into_iter()
                    .find(|filter| filter.id == filter_id)
                    .ok_or(ReadDirError::FindFilter)?;

                let item_ids = self
                    .db
                    .run_filter(&filter.rules)
                    .map_err(ReadDirError::RunFilter)?;

                let item_it = item_ids.into_iter().map(|item_id| {
                    let name = self
                        .db
                        .get_item_by_id(item_id)
                        .ok_or(ReadDirError::ItemIdNotInDatabase)?
                        .name;
                    Ok((PathPurpose::ItemLink(item_id), name))
                });

                let item_it = item_it.collect::<Result<Vec<_>, _>>()?.into_iter();

                Box::new(item_it)
            }
            PathPurpose::ToolBins => {
                let my_path = std::env::args().next().expect("no program name");
                let my_path = Path::new(&my_path);
                let parent_path = my_path
                    .parent()
                    .expect("tool bins path should always have a parent")
                    .to_path_buf();

                let names = [
                    "create-item",
                    "create-item-relationship",
                    "create-relationship",
                    "create-filter",
                ];

                Box::new(names.into_iter().map(move |name| {
                    (
                        PathPurpose::PassthroughPath(parent_path.join(name)),
                        name.to_string(),
                    )
                }))
            }
            PathPurpose::Socket
            | PathPurpose::ItemLink(_)
            | PathPurpose::ItemId(_)
            | PathPurpose::ItemName(_)
            | PathPurpose::RelationshipId(_)
            | PathPurpose::RelationshipFromName(_)
            | PathPurpose::RelationshipToName(_) => return Err(ReadDirError::NotADirectory),
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
            PathPurpose::PassthroughPath(p) => {
                let it = fs::read_dir(p).map_err(ReadDirError::ReadDbDir)?.map(
                    |item| -> Result<(PathPurpose, String), String> {
                        let item = item.map_err(|e| e.to_string())?;
                        Ok((
                            PathPurpose::PassthroughPath(item.path()),
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
        let parsed_path = self
            .parse_path(path)
            .map_err(|x| ReadDirError::ParsePath(Box::new(x)))?;
        let dir_it = self.list_dir_contents(parsed_path)?.collect::<Vec<_>>();
        let dir_it = dir_it.into_iter().map(|item| {
            let ret = match path_purpose_to_filetype(&item.0, &self.db)
                .map_err(ReadDirError::GetFiletype)?
            {
                Filetype::Dir => DirEntry::Dir(item.1.into()),
                Filetype::Link => DirEntry::Link(item.1.into()),
                Filetype::File(_) => DirEntry::File(item.1.into()),
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

        // Special case for content folder. Usually we can just list the contents of a directory,
        // and compare the input path with the listed contents as a way to check if the path is
        // valid. In content directories we allow creation of files, and so must return a
        // passthrough path whether or not the file exists
        let parsed_parent = self.parse_path(parent)?;
        if let PathPurpose::PassthroughPath(passthrough_path) = &parsed_parent {
            let ret = passthrough_path.join(name);
            return Ok(PathPurpose::PassthroughPath(ret));
        }

        let Some(item) = self
            .list_dir_contents(parsed_parent)?
            .find(|item| item.1 == name)
        else {
            return Ok(PathPurpose::Unknown);
        };

        Ok(item.0)
    }
}
