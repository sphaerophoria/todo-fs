use rusqlite::Connection;
use std::{
    fmt::{self, Write}, fs,
    path::{Path, PathBuf},
    str::FromStr,
};
use thiserror::Error;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ItemId(pub i64);

#[derive(Hash, Debug, Clone, Copy, Eq, PartialEq)]
pub struct RelationshipId(pub i64);

#[derive(Hash, Copy, Clone, Debug, Eq, PartialEq)]
pub enum RelationshipSide {
    Source,
    Dest,
}

impl fmt::Display for RelationshipSide {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RelationshipSide::Source => f.write_str("source"),
            RelationshipSide::Dest => f.write_str("dest"),
        }
    }
}

impl FromStr for RelationshipSide {
    type Err = ParseRelationshipSideError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "source" => Ok(RelationshipSide::Source),
            "dest" => Ok(RelationshipSide::Dest),
            _ => Err(ParseRelationshipSideError),
        }
    }
}

#[derive(Debug, Error)]
#[error("failed to parse relationship side")]
pub struct ParseRelationshipSideError;

impl RelationshipSide {
    fn from_i64(num: i64) -> Result<RelationshipSide, ParseRelationshipSideError> {
        let num = match num {
            0 => RelationshipSide::Source,
            1 => RelationshipSide::Dest,
            _ => return Err(ParseRelationshipSideError),
        };
        Ok(num)
    }

    fn as_i64(&self) -> i64 {
        match self {
            RelationshipSide::Source => 0,
            RelationshipSide::Dest => 1,
        }
    }
}

#[derive(Debug)]
pub struct Relationship {
    pub from_name: String,
    pub to_name: String,
    pub id: RelationshipId,
}

#[derive(Debug)]
pub struct ItemRelationship {
    pub id: RelationshipId,
    pub side: RelationshipSide,
    pub sibling: ItemId,
}

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
pub enum DeleteItemError {
    #[error("failed to start transaction")]
    StartTransaction(#[source] rusqlite::Error),
    #[error("failed to delete item")]
    DeleteItem(#[source] rusqlite::Error),
    #[error("failed to delete item relationships")]
    DeleteItemRelationships(#[source] rusqlite::Error),
    #[error("failed to remove item from disk")]
    RemoveItemPath(#[source] std::io::Error),
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
    #[error("failed to enable foreign key checks")]
    EnableForeignKeys(#[source] rusqlite::Error),
    #[error("failed to commit transactions")]
    CommitTransaction(#[source] rusqlite::Error),
    #[error("failed to create no relationships filters table")]
    UpgradeDb(#[source] UpgradeDbError),
}

#[derive(Debug, Error)]
pub enum UpgradeDbError {
    #[error("failed to get version")]
    GetVersion(#[source] QueryError),
    #[error("failed to create files table")]
    CreateFilesTable(#[source] rusqlite::Error),
    #[error("failed to create relationships table")]
    CreateRelationshipsTable(#[source] rusqlite::Error),
    #[error("failed to create item relationships table")]
    CreateItemRelationshipsTable(#[source] rusqlite::Error),
    #[error("failed to create filters table")]
    CreateFiltersTable(#[source] rusqlite::Error),
    #[error("failed to create no relationships filters table")]
    CreateNoRelationshipsFilterTable(#[source] rusqlite::Error),
    #[error("failed to set user version")]
    SetUserVersion(#[source] rusqlite::Error),
    #[error("failed to update v1 to v2 schema")]
    UpgradeV1ToV2(#[source] rusqlite::Error),
}

#[derive(Debug, Error)]
pub enum AddRelationshipError {
    #[error("failed to check if relationship already exists")]
    FindRelationship(#[source] QueryError),
    #[error("relationship already exists")]
    AlreadyExists(RelationshipId),
    #[error("failed to start transaction")]
    StartTransaction(#[source] rusqlite::Error),
    #[error("failed to insert relationship")]
    InsertRelationship(#[source] rusqlite::Error),
    #[error("failed to commit transaction")]
    CommitTransaction(#[source] rusqlite::Error),
}

#[derive(Debug, Error)]
pub enum AddItemRelationshipError {
    #[error("failed to start transaction")]
    StartTransaction(#[source] rusqlite::Error),
    #[error("failed to insert relationship")]
    InsertRelationship(#[source] rusqlite::Error),
    #[error("failed to commit transaction")]
    CommitTransaction(#[source] rusqlite::Error),
}

#[derive(Debug, Error)]
pub enum AddFilterError {
    #[error("failed to start transaction")]
    StartTransaction(#[source] rusqlite::Error),
    #[error("failed to insert filter")]
    InsertFilter(#[source] rusqlite::Error),
    #[error("failed to insert rule")]
    InsertRule(#[source] rusqlite::Error),
    #[error("failed to insert root filter")]
    InsertRootFilter(#[source] rusqlite::Error),
    #[error("failed to commit transaction")]
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

#[derive(Debug, Error)]
pub enum GetItemsError {
    #[error("failed to query items")]
    QueryItems(#[source] QueryError),
    #[error("failed to get relationships for item")]
    GetRelationships(#[source] QueryError),
}

#[derive(Debug, Error)]
pub enum GetFiltersError {
    #[error("failed to start transaction")]
    StartTransaction(#[source] rusqlite::Error),
    #[error("failed to query filters")]
    QueryFilters(#[source] QueryError),
    #[error("failed to query rules")]
    QueryRules(#[source] QueryError),
    #[error("invalid relationship side")]
    InvalidRelationshipSide(#[source] ParseRelationshipSideError),
}

#[derive(Debug, Error)]
pub enum GetRootFiltersError {
    #[error("failed to prepare statement")]
    Prepare(#[source] rusqlite::Error),
    #[error("failed to execute query")]
    Query(#[source] rusqlite::Error),
    #[error("failed to get filter id from query")]
    Map(#[source] rusqlite::Error),
    #[error("failed to resolve filters")]
    ResolveFilters(#[from] GetFiltersError),
}

#[derive(Debug, Error)]
pub enum GetConditionalFiltersError {
    #[error("failed to prepare statement")]
    Prepare(#[source] rusqlite::Error),
    #[error("failed to execute query")]
    Query(#[source] rusqlite::Error),
    #[error("failed to get filter ids from query")]
    Map(#[source] rusqlite::Error),
    #[error("failed to match condition to id")]
    MatchId,
    #[error("failed to resolve filters")]
    ResolveFilters(#[from] GetFiltersError),
}

#[derive(Debug)]
pub struct Db {
    item_path: PathBuf,
    connection: Connection,
}

pub struct ItemFilter {
    to_run: ConditionSetId,
    name: String,
    conditions: Vec<Condition>,
}

impl ItemFilter {
    pub fn filter_to_run(&self) -> ConditionSetId {
        self.to_run
    }

    pub fn matches(&self, item_id: ItemId, db: &Db) -> Result<bool, QueryError> {
        Ok(db.run_filter(&self.conditions, Some(item_id))?.contains(&item_id))
    }

    pub fn name(&self) -> &str {
        &self.name
    }
}

// NOTE: Minor optimization. Instead of generating a string from the condition, we can directly
// push the sql content into whoever the content should be written. To do this we need to implement
// Display on some struct, so we make a private struct that implements the trait
struct ConditionSqlGenerator<'a> {
    condition: &'a Condition,
    item_context: Option<ItemId>,
}

impl fmt::Display for ConditionSqlGenerator<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let side_to_condition_str = |side: &RelationshipSide| match side {
            RelationshipSide::Dest => "item_relationships.to_id = files.id",
            RelationshipSide::Source => "item_relationships.from_id = files.id",
        };
        let side_to_other_side_id_str = |side: &RelationshipSide| match side {
            RelationshipSide::Dest => "item_relationships.from_id",
            RelationshipSide::Source => "item_relationships.to_id",
        };
        match self.condition {
            Condition::NoRelationship(side, id) => {
                let side_condition_str = side_to_condition_str(side);
                let id_i64 = id.0;

                write!(f, "files.id not in (SELECT files.id FROM files JOIN item_relationships ON {side_condition_str} AND relationship_id = {id_i64})")?;
            }
            Condition::HasRelationshipWithVariableItem(side, relationship_id) => {
                let side_condition_str = side_to_condition_str(side);
                let other_side_id_str = side_to_other_side_id_str(side);
                let item_id_i64 = self.item_context.unwrap().0;
                let relationshipid_i64 = relationship_id.0;
                write!(f, "files.id in (SELECT files.id FROM files JOIN item_relationships ON {side_condition_str} AND relationship_id = {relationshipid_i64} AND {other_side_id_str} = {item_id_i64})")?;
            }
            Condition::NoRelationshipWithSpecificItem(item_id, side, relationship_id) => {
                let side_condition_str = side_to_condition_str(side);
                let other_side_id_str = side_to_other_side_id_str(side);
                let item_id_i64 = item_id.0;
                let relationshipid_i64 = relationship_id.0;
                write!(f, "files.id not in (SELECT files.id FROM files JOIN item_relationships ON {side_condition_str} AND relationship_id = {relationshipid_i64} AND {other_side_id_str} = {item_id_i64})")?;
            }
        }

        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Condition {
    NoRelationship(RelationshipSide, RelationshipId),
    // FIXME: Should be variable item_id
    HasRelationshipWithVariableItem(RelationshipSide, RelationshipId),
    NoRelationshipWithSpecificItem(ItemId, RelationshipSide, RelationshipId),
}

impl Condition {
    fn sql(&self, item_id: Option<ItemId>) -> ConditionSqlGenerator {
        ConditionSqlGenerator {
            condition: self,
            item_context: item_id,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ConditionSetId(i64);

#[derive(Debug)]
pub struct ConditionSet {
    pub id: ConditionSetId,
    pub name: String,
    pub rules: Vec<Condition>,
}

fn get_version(connection: &rusqlite::Connection) -> Result<usize, QueryError> {
    let mut statement = connection
        .prepare("PRAGMA user_version")
        .map_err(QueryError::Prepare)?;

    statement
        .query_row([], |row| {
            let ret: usize = row.get(0)?;
            Ok(ret)
        })
        .map_err(QueryError::QueryMapFailed)
}

fn generate_v1_db(connection: &rusqlite::Connection) -> Result<(), UpgradeDbError> {
    connection
        .execute(
            "CREATE TABLE IF NOT EXISTS files(id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL)",
            (),
        )
        .map_err(UpgradeDbError::CreateFilesTable)?;

    connection
        .execute(
            "CREATE TABLE IF NOT EXISTS relationships(id INTEGER PRIMARY KEY, from_name TEXT NOT NULL, to_name TEXT_NOT_NULL)",
            (),
        )
        .map_err(UpgradeDbError::CreateRelationshipsTable)?;

    connection
        .execute(
            "CREATE TABLE IF NOT EXISTS filters(id INTEGER PRIMARY KEY, name TEXT_NOT_NULL)",
            (),
        )
        .map_err(UpgradeDbError::CreateFiltersTable)?;

    connection
        .execute(
            "CREATE TABLE IF NOT EXISTS no_relationship_filters(filter_id INTEGER, side INTEGER, relationship_id INTEGER,
            FOREIGN KEY(filter_id) REFERENCES filters(id),
            FOREIGN KEY(relationship_id) REFERENCES relationships(id),
            UNIQUE(filter_id, side, relationship_id))",
            (),
        )
        .map_err(UpgradeDbError::CreateNoRelationshipsFilterTable)?;

    connection
        .execute(
            "CREATE TABLE IF NOT EXISTS item_relationships(from_id INTEGER, to_id INTEGER, relationship_id INTEGER,
            FOREIGN KEY(from_id) REFERENCES files(id),
            FOREIGN KEY(to_id) REFERENCES files(id),
            FOREIGN KEY(relationship_id) REFERENCES relationships(id),
            UNIQUE(from_id, to_id, relationship_id))",
            (),
        )
        .map_err(UpgradeDbError::CreateItemRelationshipsTable)?;

    connection
        .execute("PRAGMA user_version = 1", ())
        .map_err(UpgradeDbError::SetUserVersion)?;

    Ok(())
}

fn upgrade_v1_v2(connection: &rusqlite::Connection) -> Result<(), UpgradeDbError> {
    connection
        .execute_batch(
            "
            ALTER TABLE filters RENAME TO condition_sets;
            ALTER TABLE no_relationship_filters RENAME TO no_relationship_conditions;
            ALTER TABLE no_relationship_conditions RENAME COLUMN filter_id TO condition_id;
            CREATE TABLE root_filters(id INTEGER PRIMARY KEY,
                                      FOREIGN KEY(id) REFERENCES condition_sets(id));
            INSERT INTO root_filters(id) SELECT id FROM condition_sets;
            CREATE TABLE item_filters(condition INTEGER, filter INTEGER,
                                      FOREIGN KEY(condition) REFERENCES condition_sets(id),
                                      FOREIGN KEY(filter) REFERENCES condition_sets(id));
            CREATE TABLE has_relationship_with_variable_item_conditions(
                condition_id INTEGER,
                side INTEGER,
                relationship_id INTEGER,
                FOREIGN KEY(condition_id) REFERENCES condition_sets(id),
                FOREIGN KEY(relationship_id) REFERENCES relationships(id)
                );
            CREATE TABLE no_relationship_with_specific_item_conditions(
                condition_id INTEGER,
                item_id INTEGER,
                side INTEGER,
                relationship_id INTEGER,
                FOREIGN KEY(condition_id) REFERENCES condition_sets(id),
                FOREIGN KEY(item_id) REFERENCES files(id),
                FOREIGN KEY(relationship_id) REFERENCES relationships(id)
                );
            PRAGMA user_version = 2;
            ",
        )
        .map_err(UpgradeDbError::UpgradeV1ToV2)
}

fn upgrade_db(connection: &rusqlite::Connection) -> Result<(), UpgradeDbError> {
    let current_version = get_version(connection).map_err(UpgradeDbError::GetVersion)?;
    let upgrade_fns = [generate_v1_db, upgrade_v1_v2];

    for upgrade_fn in upgrade_fns.iter().skip(current_version) {
        upgrade_fn(connection)?;
    }

    let updated_version = get_version(connection).map_err(UpgradeDbError::GetVersion)?;

    const EXPECTED_VERSION: usize = 2;
    assert_eq!(updated_version, EXPECTED_VERSION);
    Ok(())
}

/// Returns insertion row id
fn add_condition_set(transaction: &Connection, name: &str, conditions: &[Condition]) -> Result<i64, AddFilterError> {
    transaction
        .execute("INSERT INTO condition_sets(name) VALUES (?1)", [name])
        .map_err(AddFilterError::InsertFilter)?;

    let condition_set_id = transaction.last_insert_rowid();

    for condition in conditions {
        match condition {
            Condition::NoRelationship(side, relationship_id) => {
                transaction.execute("INSERT INTO no_relationship_conditions(condition_id, side, relationship_id) VALUES (?1, ?2, ?3)", [condition_set_id, side.as_i64(), relationship_id.0]).map_err(AddFilterError::InsertRule)?;
            }
            Condition::HasRelationshipWithVariableItem(side, relationship_id) => {
                transaction.execute("INSERT INTO has_relationship_with_variable_item_conditions(condition_id, side, relationship_id) VALUES (?1, ?2, ?3)", [condition_set_id, side.as_i64(), relationship_id.0]).map_err(AddFilterError::InsertRule)?;
            }
            Condition::NoRelationshipWithSpecificItem(item_id, side, relationship_id) => {
                transaction.execute("INSERT INTO no_relationship_with_specific_item_conditions(condition_id, item_id, side, relationship_id) VALUES (?1, ?2, ?3, ?4)", [condition_set_id, item_id.0, side.as_i64(), relationship_id.0]).map_err(AddFilterError::InsertRule)?;
            }
        }
    }

    Ok(condition_set_id)
}

fn load_no_relationship_conditions(transaction: &Connection, condition_set_id: ConditionSetId) -> Result<Vec<Condition>, GetFiltersError> {
    let mut statement = transaction.prepare("SELECT side, relationship_id FROM no_relationship_conditions WHERE condition_id = ?1").map_err(QueryError::Prepare)
        .map_err(GetFiltersError::QueryRules)?;

    let mut rules = Vec::new();

    let mut query = statement
        .query([condition_set_id.0])
        .map_err(QueryError::Execute)
        .map_err(GetFiltersError::QueryRules)?;

    while let Some(row) = query
        .next()
        .map_err(QueryError::QueryMapFailed)
        .map_err(GetFiltersError::QueryRules)?
    {
        let side: i64 = row
            .get(0)
            .map_err(QueryError::QueryMapFailed)
            .map_err(GetFiltersError::QueryRules)?;
        let side = RelationshipSide::from_i64(side)
            .map_err(GetFiltersError::InvalidRelationshipSide)?;

        let relationship_id: i64 = row
            .get(1)
            .map_err(QueryError::QueryMapFailed)
            .map_err(GetFiltersError::QueryRules)?;
        let relationship_id = RelationshipId(relationship_id);
        rules.push(Condition::NoRelationship(side, relationship_id));
    }

    Ok(rules)
}

fn load_has_relationship_with_variable_item_conditions(transaction: &Connection, condition_set_id: ConditionSetId) -> Result<Vec<Condition>, GetFiltersError> {
    let mut statement = transaction.prepare("SELECT side, relationship_id FROM has_relationship_with_variable_item_conditions WHERE condition_id = ?1").map_err(QueryError::Prepare)
        .map_err(GetFiltersError::QueryRules)?;

    let mut rules = Vec::new();

    let mut query = statement
        .query([condition_set_id.0])
        .map_err(QueryError::Execute)
        .map_err(GetFiltersError::QueryRules)?;

    while let Some(row) = query
        .next()
        .map_err(QueryError::QueryMapFailed)
        .map_err(GetFiltersError::QueryRules)?
    {
        let side: i64 = row
            .get(0)
            .map_err(QueryError::QueryMapFailed)
            .map_err(GetFiltersError::QueryRules)?;
        let side = RelationshipSide::from_i64(side)
            .map_err(GetFiltersError::InvalidRelationshipSide)?;

        let relationship_id: i64 = row
            .get(1)
            .map_err(QueryError::QueryMapFailed)
            .map_err(GetFiltersError::QueryRules)?;
        let relationship_id = RelationshipId(relationship_id);
        rules.push(Condition::HasRelationshipWithVariableItem(side, relationship_id));
    }

    Ok(rules)
}

fn load_no_relationship_with_specific_item_conditions(transaction: &Connection, condition_set_id: ConditionSetId) -> Result<Vec<Condition>, GetFiltersError> {
    let mut statement = transaction.prepare("SELECT item_id, side, relationship_id FROM no_relationship_with_specific_item_conditions WHERE condition_id = ?1").map_err(QueryError::Prepare)
        .map_err(GetFiltersError::QueryRules)?;

    let mut rules = Vec::new();

    let mut query = statement
        .query([condition_set_id.0])
        .map_err(QueryError::Execute)
        .map_err(GetFiltersError::QueryRules)?;

    while let Some(row) = query
        .next()
        .map_err(QueryError::QueryMapFailed)
        .map_err(GetFiltersError::QueryRules)?
    {
        let item_id: i64 = row
            .get(0)
            .map_err(QueryError::QueryMapFailed)
            .map_err(GetFiltersError::QueryRules)?;
        let item_id = ItemId(item_id);

        let side: i64 = row
            .get(1)
            .map_err(QueryError::QueryMapFailed)
            .map_err(GetFiltersError::QueryRules)?;
        let side = RelationshipSide::from_i64(side)
            .map_err(GetFiltersError::InvalidRelationshipSide)?;

        let relationship_id: i64 = row
            .get(2)
            .map_err(QueryError::QueryMapFailed)
            .map_err(GetFiltersError::QueryRules)?;
        let relationship_id = RelationshipId(relationship_id);
        rules.push(Condition::NoRelationshipWithSpecificItem(item_id, side, relationship_id));
    }

    Ok(rules)
}

#[derive(Debug)]
pub struct DbItem {
    pub path: PathBuf,
    pub id: ItemId,
    pub relationships: Vec<ItemRelationship>,
    pub name: String,
}

impl Db {
    pub fn new(path: PathBuf) -> Result<Db, OpenDbError> {
        if !path.exists() {
            fs::create_dir_all(&path).map_err(OpenDbError::CreateFilesDir)?;
        }

        let sqlite_path = path.join("metadata.db");
        let mut connection = Connection::open(sqlite_path).map_err(OpenDbError::OpenConnection)?;

        // NOTE: cannot enable foreign keys on transaction
        connection
            .execute("PRAGMA foreign_keys = ON", ())
            .map_err(OpenDbError::EnableForeignKeys)?;

        let transaction = connection
            .transaction()
            .map_err(OpenDbError::StartTransaction)?;

        upgrade_db(&transaction).map_err(OpenDbError::UpgradeDb)?;

        transaction
            .commit()
            .map_err(OpenDbError::CommitTransaction)?;
        let item_path = path.join("items");
        Ok(Db {
            item_path,
            connection,
        })
    }

    pub fn create_item(&mut self, name: &str) -> Result<ItemId, CreateItemError> {
        let transaction = self
            .connection
            .transaction()
            .map_err(CreateItemError::StartTransaction)?;
        transaction
            .execute("INSERT INTO files(name) VALUES (?1)", [name])
            .map_err(CreateItemError::InsertItem)?;
        let id = transaction.last_insert_rowid();

        let item_path = self.item_path.join(id.to_string());
        if item_path.exists() {
            return Err(CreateItemError::ItemExists);
        }

        fs::create_dir_all(item_path).map_err(CreateItemError::CreateContentFolder)?;

        transaction
            .commit()
            .map_err(CreateItemError::CommitTransaction)?;
        Ok(ItemId(id))
    }

    pub fn delete_item(&mut self, id: ItemId) -> Result<(), DeleteItemError> {
        let transaction = self
            .connection
            .transaction()
            .map_err(DeleteItemError::StartTransaction)?;

        transaction
            .execute(
                "DELETE FROM item_relationships WHERE from_id = ?1 OR to_id = ?1",
                [id.0],
            )
            .map_err(DeleteItemError::DeleteItemRelationships)?;

        transaction
            .execute("DELETE FROM files WHERE id = ?1", [id.0])
            .map_err(DeleteItemError::DeleteItem)?;

        let item_path = self.item_path.join(id.0.to_string());
        fs::remove_dir_all(item_path).map_err(DeleteItemError::RemoveItemPath)?;

        transaction
            .commit()
            .map_err(DeleteItemError::CommitTransaction)?;
        Ok(())
    }

    pub fn add_relationship(
        &mut self,
        from_name: &str,
        to_name: &str,
    ) -> Result<RelationshipId, AddRelationshipError> {
        if let Some(id) = self
            .find_relationship(from_name, to_name)
            .map_err(AddRelationshipError::FindRelationship)?
        {
            return Err(AddRelationshipError::AlreadyExists(id));
        }

        let transaction = self
            .connection
            .transaction()
            .map_err(AddRelationshipError::StartTransaction)?;
        transaction
            .execute(
                "INSERT INTO relationships(from_name, to_name) VALUES (?1, ?2)",
                [from_name, to_name],
            )
            .map_err(AddRelationshipError::InsertRelationship)?;
        let id = transaction.last_insert_rowid();

        transaction
            .commit()
            .map_err(AddRelationshipError::CommitTransaction)?;

        Ok(RelationshipId(id))
    }

    fn find_relationship(
        &mut self,
        from_name: &str,
        to_name: &str,
    ) -> Result<Option<RelationshipId>, QueryError> {
        let mut statement = self
            .connection
            .prepare("SELECT id FROM relationships WHERE from_name = ?1 OR to_name = ?1 OR from_name = ?2 OR to_name = ?2")
            .map_err(QueryError::Prepare)?;

        let item = statement
            .query_map([from_name, to_name], |row| {
                let ret: i64 = row.get(0)?;
                Ok(RelationshipId(ret))
            })
            .map_err(QueryError::Execute)?
            .next();

        item.transpose().map_err(QueryError::QueryMapFailed)
    }

    pub fn get_relationship(&self, id: RelationshipId) -> Result<Option<Relationship>, QueryError> {
        let mut statement = self
            .connection
            .prepare("SELECT id, from_name, to_name FROM relationships WHERE id = ?1")
            .map_err(QueryError::Prepare)?;

        let item = statement
            .query_map([id.0], |row| {
                let id: i64 = row.get(0)?;
                let from_name: String = row.get(1)?;
                let to_name: String = row.get(2)?;
                Ok(Relationship {
                    id: RelationshipId(id),
                    from_name,
                    to_name,
                })
            })
            .map_err(QueryError::Execute)?
            .next();

        // Option<Result<Relationship>> -> Relationship
        item.transpose().map_err(QueryError::QueryMapFailed)
    }

    pub fn get_relationships(&self) -> Result<Vec<Relationship>, QueryError> {
        let mut statement = self
            .connection
            .prepare("SELECT id, from_name, to_name FROM relationships")
            .map_err(QueryError::Prepare)?;

        let ret = statement
            .query_map([], |row| {
                let id: i64 = row.get(0)?;
                let from_name: String = row.get(1)?;
                let to_name: String = row.get(2)?;
                let id = RelationshipId(id);
                Ok(Relationship {
                    id,
                    from_name,
                    to_name,
                })
            })
            .map_err(QueryError::Execute)?
            .map(|x| x.map_err(QueryError::QueryMapFailed))
            .collect();

        // Rust requires binding to prevent it from thinking it's returning a reference
        #[allow(clippy::let_and_return)]
        ret
    }

    pub fn add_item_relationship(
        &mut self,
        from_id: ItemId,
        to_id: ItemId,
        relationship_id: RelationshipId,
    ) -> Result<(), AddItemRelationshipError> {
        let transaction = self
            .connection
            .transaction()
            .map_err(AddItemRelationshipError::StartTransaction)?;
        transaction
            .execute("INSERT INTO item_relationships(from_id, to_id, relationship_id) VALUES (?1, ?2, ?3)", [from_id.0, to_id.0, relationship_id.0])
            .map_err(AddItemRelationshipError::InsertRelationship)?;

        transaction
            .commit()
            .map_err(AddItemRelationshipError::CommitTransaction)?;
        Ok(())
    }

    pub fn fs_root(&self) -> &Path {
        &self.item_path
    }

    pub fn add_root_filter(
        &mut self,
        name: &str,
        conditions: &[Condition],
    ) -> Result<(), AddFilterError> {
        let transaction = self
            .connection
            .transaction()
            .map_err(AddFilterError::StartTransaction)?;

        let inserted_condition_set = add_condition_set(&transaction, name, conditions)?;

        transaction
            .execute(
                "INSERT INTO root_filters(id) VALUES (?1)",
                [inserted_condition_set],
            )
            .map_err(AddFilterError::InsertRootFilter)?;

        transaction
            .commit()
            .map_err(AddFilterError::CommitTransaction)?;

        Ok(())
    }

    pub fn get_condition_sets(&mut self) -> Result<Vec<ConditionSet>, GetFiltersError> {
        let transaction = self
            .connection
            .transaction()
            .map_err(GetFiltersError::StartTransaction)?;

        let mut statement = transaction
            .prepare("SELECT id, name FROM condition_sets")
            .map_err(QueryError::Prepare)
            .map_err(GetFiltersError::QueryFilters)?;

        let ret: Result<Vec<ConditionSet>, QueryError> = statement
            .query_map((), |row| {
                let id: i64 = row.get(0)?;
                let name: String = row.get(1)?;

                Ok(ConditionSet {
                    id: ConditionSetId(id),
                    name,
                    rules: Vec::new(),
                })
            })
            .map_err(QueryError::Execute)
            .map_err(GetFiltersError::QueryFilters)?
            .map(|x| x.map_err(QueryError::QueryMapFailed))
            .collect();

        let mut ret = ret.map_err(GetFiltersError::QueryFilters)?;

        for item in &mut ret {
            let mut rules = load_no_relationship_conditions(&transaction, item.id).unwrap();
            rules.extend(load_has_relationship_with_variable_item_conditions(&transaction, item.id).unwrap());
            rules.extend(load_no_relationship_with_specific_item_conditions(&transaction, item.id).unwrap());
            item.rules = rules;
        }

        Ok(ret)
    }

    pub fn run_filter(&self, conditions: &[Condition], item_id: Option<ItemId>) -> Result<Vec<ItemId>, QueryError> {
        let mut query_string = "SELECT files.id FROM files ".to_string();

        let mut conditions_it = conditions.iter();
        if let Some(condition) = conditions_it.next() {
            write!(query_string, "WHERE ({}) ", condition.sql(item_id)).unwrap();
        }

        for condition in conditions_it {
            write!(query_string, "AND ({}) ", condition.sql(item_id)).unwrap();
        }

        println!("{}", query_string);

        let mut statement = self
            .connection
            .prepare(&query_string)
            .map_err(QueryError::Prepare)?;

        let ret: Result<Vec<_>, QueryError> = statement
            .query_map([], |row| {
                let id: i64 = row.get(0)?;
                Ok(ItemId(id))
            })
            .map_err(QueryError::Execute)?
            .map(|x| x.map_err(QueryError::QueryMapFailed))
            .collect();

        ret
    }

    pub fn get_root_filters(&mut self) -> Result<Vec<ConditionSet>, GetRootFiltersError> {
        let root_filter_ids: Vec<ConditionSetId> = {
            let mut filters_statement = self
                .connection
                .prepare("SELECT id FROM root_filters")
                .map_err(GetRootFiltersError::Prepare)?;

            // Rust does not handle lifetimes correctly without the let binding
            #[allow(clippy::let_and_return)]
            let ret = filters_statement
                .query_map((), |row| {
                    let id = ConditionSetId(row.get(0)?);
                    Ok(id)
                })
                .map_err(GetRootFiltersError::Query)?
                .collect::<Result<_, _>>()
                .map_err(GetRootFiltersError::Map)?;
            ret
        };

        let ret = self
            .get_condition_sets()?
            .into_iter()
            .filter(|filter| root_filter_ids.contains(&filter.id))
            .collect();
        Ok(ret)
    }

    pub fn add_item_filter(&mut self, name: &str, conditions: &[Condition], filters: &[Condition]) -> Result<(), AddFilterError> {
        let transaction = self
            .connection
            .transaction()
            .map_err(AddFilterError::StartTransaction)?;

        // FIXME: Unique error types
        let condition_id = add_condition_set(&transaction, name, conditions).unwrap();
        let filter_id = add_condition_set(&transaction, name, filters).unwrap();

        transaction
            .execute(
                "INSERT INTO item_filters(condition, filter) VALUES (?1, ?2)",
                [condition_id, filter_id],
            )
            .map_err(AddFilterError::InsertRootFilter)?;

        transaction
            .commit()
            .map_err(AddFilterError::CommitTransaction)?;

        Ok(())
    }

    pub fn get_item_filters(&mut self) -> Result<Vec<ItemFilter>, GetConditionalFiltersError> {
        let item_filter_ids: Vec<(ConditionSetId, ConditionSetId)> = {
            let mut filters_statement = self
                .connection
                .prepare("SELECT condition, filter FROM item_filters")
                .map_err(GetConditionalFiltersError::Prepare)?;

            // Rust does not handle lifetimes correctly without let binding
            #[allow(clippy::let_and_return)]
            let ret = filters_statement
                .query_map((), |row| {
                    let condition_id = ConditionSetId(row.get(0)?);
                    let filters_to_run = ConditionSetId(row.get(1)?);
                    Ok((condition_id, filters_to_run))
                })
                .map_err(GetConditionalFiltersError::Query)?
                .collect::<Result<_, _>>()
                .map_err(GetConditionalFiltersError::Map)?;
            ret
        };

        let all_filters = self.get_condition_sets()?;
        let mut ret = Vec::new();
        for (condition_id, filters_to_run) in item_filter_ids {
            let conditions = all_filters
                .iter()
                .find(|filter| condition_id == filter.id)
                .ok_or(GetConditionalFiltersError::MatchId)?;
            ret.push(ItemFilter {
                to_run: filters_to_run,
                // FIXME: Probably needless clones, should be 1-1 mapping between item_filter_ids
                // and all_filters
                name: conditions.name.clone(),
                conditions: conditions.rules.clone(),
            })
        }
        Ok(ret)
    }

    pub fn content_folder_for_id(&self, id: ItemId) -> Result<PathBuf, std::io::Error> {
        self.item_path.join(id.0.to_string()).canonicalize()
    }

    pub fn get_sibling_id(
        &self,
        id: ItemId,
        side: RelationshipSide,
        relationship_id: RelationshipId,
        sibling_name: &str,
    ) -> Result<Option<ItemId>, QueryError> {
        let join_str = match side {
            RelationshipSide::Dest => {
                "INNER JOIN item_relationships ON us_files.id = item_relationships.to_id LEFT JOIN files them_files ON them_files.id = item_relationships.from_id"
            }
            RelationshipSide::Source => {
                "INNER JOIN item_relationships ON us_files.id = item_relationships.from_id LEFT JOIN files them_files ON them_files.id = item_relationships.to_id"
            }
        };

        let query = format!("SELECT them_files.id FROM files us_files {join_str} LEFT JOIN relationships ON item_relationships.relationship_id = relationships.id WHERE us_files.id = ?1 AND them_files.name = ?2 AND relationships.id = ?3");

        let mut statement = self
            .connection
            .prepare(&query)
            .map_err(QueryError::Prepare)?;
        let mut query = statement
            .query_map(
                rusqlite::params![id.0, sibling_name, relationship_id.0],
                |row| {
                    let id: i64 = row.get(0)?;
                    Ok(ItemId(id))
                },
            )
            .map_err(QueryError::Execute)?;

        // Option<Result<..>> -> Result<Option<...>>
        let first = query
            .next()
            .transpose()
            .map_err(QueryError::QueryMapFailed)?;
        let second = query
            .next()
            .transpose()
            .map_err(QueryError::QueryMapFailed)?;

        if second.is_some() {
            panic!("Multiple items matched :(");
        }

        Ok(first)
    }

    pub fn get_item_by_id(&self, id: ItemId) -> Option<DbItem> {
        // FIXME: Don't query the whole database for every item lookup idiot
        self.get_items()
            .into_iter()
            .flatten()
            .find(|item| item.id == id)
    }

    pub fn get_items(&self) -> Result<Vec<DbItem>, GetItemsError> {
        let mut statement = self
            .connection
            .prepare("SELECT id, name FROM files")
            .map_err(QueryError::Prepare)
            .map_err(GetItemsError::QueryItems)?;

        struct Item {
            id: ItemId,
            name: String,
        }
        let items: Vec<Item> = statement
            .query_map([], |row| {
                let id: i64 = row.get(0)?;
                let id = ItemId(id);
                Ok(Item {
                    id,
                    name: row.get(1)?,
                })
            })
            .map_err(QueryError::Execute)
            .map_err(GetItemsError::QueryItems)?
            .map(|x| {
                x.map_err(QueryError::QueryMapFailed)
                    .map_err(GetItemsError::QueryItems)
            })
            .collect::<Result<Vec<Item>, GetItemsError>>()?;

        let mut statement = self
            .connection
            .prepare("SELECT from_id, to_id, relationship_id FROM item_relationships")
            .map_err(QueryError::Prepare)
            .map_err(GetItemsError::GetRelationships)?;

        struct DbRelationship {
            from_id: ItemId,
            to_id: ItemId,
            relationship_id: RelationshipId,
        }

        let item_relationships: Vec<DbRelationship> = statement
            .query_map([], |row| {
                let from_id: i64 = row.get(0)?;
                let to_id: i64 = row.get(1)?;
                let relationship_id: i64 = row.get(2)?;
                Ok(DbRelationship {
                    from_id: ItemId(from_id),
                    to_id: ItemId(to_id),
                    relationship_id: RelationshipId(relationship_id),
                })
            })
            .map_err(QueryError::Execute)
            .map_err(GetItemsError::GetRelationships)?
            .map(|x| {
                x.map_err(QueryError::QueryMapFailed)
                    .map_err(GetItemsError::GetRelationships)
            })
            .collect::<Result<Vec<DbRelationship>, GetItemsError>>()?;

        let mut ret = Vec::new();
        for item in items {
            let mut relationships = Vec::new();
            for relationship in &item_relationships {
                if relationship.from_id == item.id {
                    relationships.push(ItemRelationship {
                        id: relationship.relationship_id,
                        sibling: relationship.to_id,
                        side: RelationshipSide::Source,
                    });
                }
                if relationship.to_id == item.id {
                    relationships.push(ItemRelationship {
                        id: relationship.relationship_id,
                        sibling: relationship.from_id,
                        side: RelationshipSide::Dest,
                    });
                }
            }

            ret.push(DbItem {
                path: self.item_path.join(item.id.0.to_string()),
                id: item.id,
                relationships,
                name: item.name,
            })
        }
        Ok(ret)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use tempfile::TempDir;

    struct Fixture {
        temp_dir: TempDir,
        db: Db,
    }

    fn create_fixture() -> Fixture {
        let temp_dir = tempfile::tempdir().expect("failed to create db dir");
        let db = Db::new(temp_dir.path().into()).expect("failed to create db");
        Fixture { temp_dir, db }
    }

    #[test]
    fn open_empty_db() {
        create_fixture();
    }

    #[test]
    fn open_populated_db() {
        let fixture = create_fixture();
        let db = Db::new(fixture.temp_dir.path().into()).expect("failed to create db");
    }

    #[test]
    fn create_new_item() {
        let mut fixture = create_fixture();
        let id = fixture
            .db
            .create_item("test")
            .expect("failed to create item");

        let retrieved_item = fixture.db.get_item_by_id(id).expect("item should be in db");

        assert!(retrieved_item.path.exists());
        assert!(retrieved_item.path.is_dir());
        assert_eq!(retrieved_item.id, id);
        assert!(retrieved_item.relationships.is_empty());
        assert_eq!(retrieved_item.name, "test");
    }

    #[test]
    fn create_new_item_already_exists_on_disk() {
        let mut fixture = create_fixture();

        std::fs::create_dir_all(fixture.temp_dir.path().join("items/1"))
            .expect("failed to create conflicting dir");

        match fixture.db.create_item("test") {
            Err(CreateItemError::ItemExists) => (),
            _ => panic!("Unexpected response to creating existing item"),
        };
    }

    #[test]
    fn add_relationship_success() {
        let mut fixture = create_fixture();
        fixture
            .db
            .add_relationship("parents", "children")
            .expect("failed to create relationship");
    }

    #[test]
    fn add_relationship_already_exists() {
        let mut fixture = create_fixture();
        fixture
            .db
            .add_relationship("parents", "children")
            .expect("failed to create relationship");
        let Err(AddRelationshipError::AlreadyExists(_)) =
            fixture.db.add_relationship("parents", "new_key")
        else {
            panic!("expected already exists");
        };

        let Err(AddRelationshipError::AlreadyExists(_)) =
            fixture.db.add_relationship("new_key", "parents")
        else {
            panic!("expected already exists");
        };

        let Err(AddRelationshipError::AlreadyExists(_)) =
            fixture.db.add_relationship("children", "new_key")
        else {
            panic!("expected already exists");
        };

        let Err(AddRelationshipError::AlreadyExists(_)) =
            fixture.db.add_relationship("new_key", "children")
        else {
            panic!("expected already exists");
        };

        fixture
            .db
            .add_relationship("new_key", "new_key_2")
            .expect("failed to create releationship with new key");
    }

    #[test]
    fn get_relationship() {
        let mut fixture = create_fixture();
        let relationship_id = fixture
            .db
            .add_relationship("parents", "children")
            .expect("failed to create relationship");
        let relationship_id_2 = fixture
            .db
            .add_relationship("parents2", "children2")
            .expect("failed to create relationship");

        let relationship_1 = fixture
            .db
            .get_relationship(relationship_id)
            .expect("failed to get relationship")
            .expect("relationship does not exist");
        assert_eq!(relationship_1.from_name, "parents");
        assert_eq!(relationship_1.to_name, "children");
    }

    #[test]
    fn get_all_relationship() {
        let mut fixture = create_fixture();
        let relationship_id = fixture
            .db
            .add_relationship("parents", "children")
            .expect("failed to create relationship");
        let relationship_id_2 = fixture
            .db
            .add_relationship("parents2", "children2")
            .expect("failed to create relationship");

        use std::collections::HashMap;

        let items: HashMap<String, String> = fixture
            .db
            .get_relationships()
            .expect("failed to get relationships")
            .into_iter()
            .map(|item| (item.from_name, item.to_name))
            .collect();

        assert_eq!(items.get("parents").map(|x| x.as_ref()), Some("children"));
        assert_eq!(items.get("parents2").map(|x| x.as_ref()), Some("children2"));
    }

    #[test]
    fn add_item_relationship() {
        let mut fixture = create_fixture();
        let relationship_id = fixture
            .db
            .add_relationship("parents", "children")
            .expect("failed to create relationship");
        let item_1 = fixture
            .db
            .create_item("test")
            .expect("failed to create item");
        let item_2 = fixture
            .db
            .create_item("test2")
            .expect("failed to create item");
        fixture
            .db
            .add_item_relationship(item_1, item_2, relationship_id)
            .expect("failed to create relationship");
        let retrieved_1 = fixture
            .db
            .get_item_by_id(item_1)
            .expect("failed to retrieve relationship");
        let retrieved_2 = fixture
            .db
            .get_item_by_id(item_2)
            .expect("failed to retrieve relationship");

        assert_eq!(retrieved_1.relationships.len(), 1);
        assert_eq!(retrieved_1.relationships[0].id, relationship_id);
        assert_eq!(retrieved_1.relationships[0].side, RelationshipSide::Source);
        assert_eq!(retrieved_1.relationships[0].sibling, item_2);

        assert_eq!(retrieved_2.relationships.len(), 1);
        assert_eq!(retrieved_2.relationships[0].id, relationship_id);
        assert_eq!(retrieved_2.relationships[0].side, RelationshipSide::Dest);
        assert_eq!(retrieved_2.relationships[0].sibling, item_1);
    }

    #[test]
    fn add_item_relationship_already_exists() {
        let mut fixture = create_fixture();
        let relationship_id = fixture
            .db
            .add_relationship("parents", "children")
            .expect("failed to create relationship");
        let item_1 = fixture
            .db
            .create_item("test")
            .expect("failed to create item");
        let item_2 = fixture
            .db
            .create_item("test2")
            .expect("failed to create item");

        fixture
            .db
            .add_item_relationship(item_1, item_2, relationship_id)
            .expect("failed to create relationship");
        let Err(AddItemRelationshipError::InsertRelationship(_)) = fixture
            .db
            .add_item_relationship(item_1, item_2, relationship_id)
        else {
            panic!("expected insertion error");
        };
    }

    #[test]
    fn item_relationships_from_id_foreign_key() {
        let mut fixture = create_fixture();
        let relationship_id = fixture
            .db
            .add_relationship("parents", "children")
            .expect("failed to create relationship");
        let item_1 = fixture
            .db
            .create_item("test")
            .expect("failed to create item");
        let item_2 = fixture
            .db
            .create_item("test2")
            .expect("failed to create item");

        let Err(AddItemRelationshipError::InsertRelationship(_)) = fixture
            .db
            .add_item_relationship(ItemId(99), item_2, relationship_id)
        else {
            panic!("expected insertion error");
        };
    }

    #[test]
    fn item_relationships_to_id_foreign_key() {
        let mut fixture = create_fixture();
        let relationship_id = fixture
            .db
            .add_relationship("parents", "children")
            .expect("failed to create relationship");
        let item_1 = fixture
            .db
            .create_item("test")
            .expect("failed to create item");
        let item_2 = fixture
            .db
            .create_item("test2")
            .expect("failed to create item");

        let Err(AddItemRelationshipError::InsertRelationship(_)) = fixture
            .db
            .add_item_relationship(item_1, ItemId(99), relationship_id)
        else {
            panic!("expected insertion error");
        };
    }

    #[test]
    fn item_relationships_relationship_id_foreign_key() {
        let mut fixture = create_fixture();
        let relationship_id = fixture
            .db
            .add_relationship("parents", "children")
            .expect("failed to create relationship");
        let item_1 = fixture
            .db
            .create_item("test")
            .expect("failed to create item");
        let item_2 = fixture
            .db
            .create_item("test2")
            .expect("failed to create item");

        let Err(AddItemRelationshipError::InsertRelationship(_)) = fixture
            .db
            .add_item_relationship(item_1, item_2, RelationshipId(99))
        else {
            panic!("expected insertion error");
        };
    }

    #[test]
    fn lookup_present_item_id_from_dest_sibling() {
        let mut fixture = create_fixture();
        let relationship_id = fixture
            .db
            .add_relationship("parents", "children")
            .expect("failed to create relationship");
        let item_1 = fixture
            .db
            .create_item("test")
            .expect("failed to create item");
        let item_2 = fixture
            .db
            .create_item("test2")
            .expect("failed to create item");

        fixture
            .db
            .add_item_relationship(item_1, item_2, relationship_id)
            .expect("failed to create relationship");
        let item_id = fixture
            .db
            .get_sibling_id(item_1, RelationshipSide::Source, relationship_id, "test2")
            .expect("failed to find item id");
        assert_eq!(item_id, Some(item_2));
    }

    #[test]
    fn lookup_missing_item_id_from_dest_sibling_no_sibling_name() {
        let mut fixture = create_fixture();
        let relationship_id = fixture
            .db
            .add_relationship("parents", "children")
            .expect("failed to create relationship");
        let item_1 = fixture
            .db
            .create_item("test")
            .expect("failed to create item");
        let item_2 = fixture
            .db
            .create_item("test2")
            .expect("failed to create item");

        fixture
            .db
            .add_item_relationship(item_1, item_2, relationship_id)
            .expect("failed to create relationship");
        let Ok(None) =
            fixture
                .db
                .get_sibling_id(item_1, RelationshipSide::Source, relationship_id, "invalid")
        else {
            panic!("did not expect to find sibling");
        };
    }

    #[test]
    fn lookup_missing_item_id_from_dest_sibling_no_relationship() {
        let mut fixture = create_fixture();
        let relationship_id = fixture
            .db
            .add_relationship("parents", "children")
            .expect("failed to create relationship");
        let item_1 = fixture
            .db
            .create_item("test")
            .expect("failed to create item");
        let item_2 = fixture
            .db
            .create_item("test2")
            .expect("failed to create item");

        fixture
            .db
            .add_item_relationship(item_1, item_2, relationship_id)
            .expect("failed to create relationship");
        let Ok(None) = fixture.db.get_sibling_id(
            item_1,
            RelationshipSide::Source,
            RelationshipId(99),
            "test2",
        ) else {
            panic!("did not expect to find sibling");
        };
    }

    #[test]
    fn lookup_missing_item_id_from_dest_sibling_no_source_id() {
        let mut fixture = create_fixture();
        let relationship_id = fixture
            .db
            .add_relationship("parents", "children")
            .expect("failed to create relationship");
        let item_1 = fixture
            .db
            .create_item("test")
            .expect("failed to create item");
        let item_2 = fixture
            .db
            .create_item("test2")
            .expect("failed to create item");

        fixture
            .db
            .add_item_relationship(item_1, item_2, relationship_id)
            .expect("failed to create relationship");
        let Ok(None) = fixture.db.get_sibling_id(
            ItemId(99),
            RelationshipSide::Source,
            relationship_id,
            "test2",
        ) else {
            panic!("did not expect to find sibling");
        };
    }

    #[test]
    fn lookup_missing_item_id_from_dest_sibling_wrong_side() {
        let mut fixture = create_fixture();
        let relationship_id = fixture
            .db
            .add_relationship("parents", "children")
            .expect("failed to create relationship");
        let item_1 = fixture
            .db
            .create_item("test")
            .expect("failed to create item");
        let item_2 = fixture
            .db
            .create_item("test2")
            .expect("failed to create item");

        fixture
            .db
            .add_item_relationship(item_1, item_2, relationship_id)
            .expect("failed to create relationship");
        let Ok(None) =
            fixture
                .db
                .get_sibling_id(item_1, RelationshipSide::Dest, relationship_id, "test2")
        else {
            panic!("did not expect to find sibling");
        };
    }

    #[test]
    fn lookup_present_item_id_from_source_sibling() {
        let mut fixture = create_fixture();
        let relationship_id = fixture
            .db
            .add_relationship("parents", "children")
            .expect("failed to create relationship");
        let item_1 = fixture
            .db
            .create_item("test")
            .expect("failed to create item");
        let item_2 = fixture
            .db
            .create_item("test2")
            .expect("failed to create item");

        fixture
            .db
            .add_item_relationship(item_1, item_2, relationship_id)
            .expect("failed to create relationship");
        fixture
            .db
            .get_sibling_id(item_2, RelationshipSide::Dest, relationship_id, "test")
            .expect("failed to find sibling");
    }

    #[test]
    fn lookup_missing_item_id_from_source_sibling_no_sibling_name() {
        let mut fixture = create_fixture();
        let relationship_id = fixture
            .db
            .add_relationship("parents", "children")
            .expect("failed to create relationship");
        let item_1 = fixture
            .db
            .create_item("test")
            .expect("failed to create item");
        let item_2 = fixture
            .db
            .create_item("test2")
            .expect("failed to create item");

        fixture
            .db
            .add_item_relationship(item_1, item_2, relationship_id)
            .expect("failed to create relationship");
        let Ok(None) =
            fixture
                .db
                .get_sibling_id(item_2, RelationshipSide::Dest, relationship_id, "invalid")
        else {
            panic!("did not expect to find sibling");
        };
    }

    #[test]
    fn lookup_missing_item_id_from_source_sibling_no_relationship() {
        let mut fixture = create_fixture();
        let relationship_id = fixture
            .db
            .add_relationship("parents", "children")
            .expect("failed to create relationship");
        let item_1 = fixture
            .db
            .create_item("test")
            .expect("failed to create item");
        let item_2 = fixture
            .db
            .create_item("test2")
            .expect("failed to create item");

        fixture
            .db
            .add_item_relationship(item_1, item_2, relationship_id)
            .expect("failed to create relationship");
        let Ok(None) =
            fixture
                .db
                .get_sibling_id(item_2, RelationshipSide::Dest, RelationshipId(99), "test")
        else {
            panic!("did not expect to find sibling");
        };
    }

    #[test]
    fn lookup_missing_item_id_from_source_sibling_no_source_id() {
        let mut fixture = create_fixture();
        let relationship_id = fixture
            .db
            .add_relationship("parents", "children")
            .expect("failed to create relationship");
        let item_1 = fixture
            .db
            .create_item("test")
            .expect("failed to create item");
        let item_2 = fixture
            .db
            .create_item("test2")
            .expect("failed to create item");

        fixture
            .db
            .add_item_relationship(item_1, item_2, relationship_id)
            .expect("failed to create relationship");
        let Ok(None) =
            fixture
                .db
                .get_sibling_id(ItemId(99), RelationshipSide::Dest, relationship_id, "test")
        else {
            panic!("did not expect to find sibling");
        };
    }

    #[test]
    fn lookup_missing_item_id_from_source_sibling_wrong_side() {
        let mut fixture = create_fixture();
        let relationship_id = fixture
            .db
            .add_relationship("parents", "children")
            .expect("failed to create relationship");
        let item_1 = fixture
            .db
            .create_item("test")
            .expect("failed to create item");
        let item_2 = fixture
            .db
            .create_item("test2")
            .expect("failed to create item");

        fixture
            .db
            .add_item_relationship(item_1, item_2, relationship_id)
            .expect("failed to create relationship");
        let Ok(None) =
            fixture
                .db
                .get_sibling_id(item_2, RelationshipSide::Source, relationship_id, "test")
        else {
            panic!("did not expect to find sibling");
        };
    }

    #[test]
    fn get_item_by_id_success() {
        let mut fixture = create_fixture();
        let relationship_id = fixture
            .db
            .add_relationship("parents", "children")
            .expect("failed to create relationship");
        let item_id = fixture
            .db
            .create_item("test")
            .expect("failed to create item");
        let item = fixture
            .db
            .get_item_by_id(item_id)
            .expect("failed to get item by id");
    }

    #[test]
    fn get_item_by_id_missing_id() {
        let mut fixture = create_fixture();
        let relationship_id = fixture
            .db
            .add_relationship("parents", "children")
            .expect("failed to create relationship");
        assert!(fixture.db.get_item_by_id(ItemId(99)).is_none());
    }

    #[test]
    fn add_filter_to_db() {
        let mut fixture = create_fixture();
        let relationship_id = fixture
            .db
            .add_relationship("parents", "children")
            .expect("failed to create relationship");

        fixture
            .db
            .add_root_filter(
                "my_filter",
                &[Condition::NoRelationship(
                    RelationshipSide::Dest,
                    relationship_id,
                )],
            )
            .expect("failed to add filter");

        let filters = fixture
            .db
            .get_root_filters()
            .expect("failed to get filters");

        assert_eq!(filters.len(), 1);
        assert_eq!(filters[0].name, "my_filter");
        assert_eq!(filters[0].rules.len(), 1);
        assert_eq!(
            filters[0].rules[0],
            Condition::NoRelationship(RelationshipSide::Dest, relationship_id)
        );
    }

    #[test]
    fn delete_item() {
        let mut fixture = create_fixture();
        let relationship_id = fixture
            .db
            .add_relationship("parents", "children")
            .expect("failed to create relationship");

        let parent_id = fixture
            .db
            .create_item("parent")
            .expect("failed to create parent");
        let child_id = fixture
            .db
            .create_item("child")
            .expect("failed to create parent");

        let child_data_path = fixture
            .temp_dir
            .path()
            .join("items")
            .join(child_id.0.to_string());
        assert!(child_data_path.exists());

        fixture
            .db
            .add_item_relationship(parent_id, child_id, relationship_id)
            .expect("failed to add item relationship");

        // Pre-deletion, parent should see a relationship with child
        let parent = fixture
            .db
            .get_item_by_id(parent_id)
            .expect("failed to get parent");
        assert_eq!(parent.relationships.len(), 1);

        fixture
            .db
            .delete_item(child_id)
            .expect("failed to delete child");
        // Child should fail to resolve after being deleted
        assert!(fixture.db.get_item_by_id(child_id).is_none());
        // Child data should be deleted
        assert!(!child_data_path.exists());

        // Post-deletion, parent should no longer see a relationship with child
        let parent = fixture
            .db
            .get_item_by_id(parent_id)
            .expect("failed to get parent");
        assert_eq!(parent.relationships.len(), 0);
    }


    // FIXME: Missing add root filter test
    // FIXME: Missing add item filter test
}
