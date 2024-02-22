use todo_fs::{
    db::{Condition, RelationshipId, ItemId},
    fuse::api::{self, ClientRequest, CreateFilterRequest},
};

use std::borrow::Borrow;
use thiserror::Error;

#[derive(Error, Debug)]
enum ArgParseError {
    #[error("missing side for no_relationship filter")]
    MissingSide,
    #[error("missing relationship id for no_relationship filter")]
    MissingRelationshipId,
    #[error("failed to parse relationship side")]
    ParseRelationshipSide,
    #[error("failed to parse relationship id")]
    ParseRelationshipId(#[source] std::num::ParseIntError),
    #[error("missing filter name")]
    MissingFilterName,
    #[error("missing filter type")]
    MissingFilterType,
    #[error("unknown filter name {0}")]
    UnknownFilter(String),
    #[error("unknown argument {0}")]
    UnknownArg(String),
}

// FIXME: Dedup with create-root-filter.rs
fn parse_filter<It: Iterator<Item = String>>(it: &mut It) -> Result<Condition, ArgParseError> {
    let filter_name = it.next().ok_or(ArgParseError::MissingFilterType)?;
    match filter_name.borrow() {
        "no_relationship" => {
            parse_no_relationship_filter(it)
        }
        "has_relationship_with_variable_item" => {
            parse_has_relationship_with_item(it)
        }
        "no_relationship_with_specific_item" => {
            parse_no_relationship_with_item(it)
        }
        _ => Err(ArgParseError::UnknownFilter(filter_name))
    }

}

fn parse_has_relationship_with_item<It: Iterator<Item = String>>(it: &mut It) -> Result<Condition, ArgParseError> {
    let side = it.next().unwrap();
    let side = side.parse().unwrap();
    let relationship_id = it.next().unwrap();
    let relationship_id = relationship_id.parse().unwrap();

    Ok(Condition::HasRelationshipWithVariableItem(side, RelationshipId(relationship_id)))
}

fn parse_no_relationship_with_item<It: Iterator<Item = String>>(it: &mut It) -> Result<Condition, ArgParseError> {
    let item_id = it.next().unwrap();
    let item_id = item_id.parse().unwrap();
    let side = it.next().unwrap();
    let side = side.parse().unwrap();
    let relationship_id = it.next().unwrap();
    let relationship_id = relationship_id.parse().unwrap();

    Ok(Condition::NoRelationshipWithSpecificItem(ItemId(item_id), side, RelationshipId(relationship_id)))
}

fn parse_no_relationship_filter<It: Iterator<Item = String>>(it: &mut It) -> Result<Condition, ArgParseError> {
    let side = it.next().ok_or(ArgParseError::MissingSide)?;
    let relationship_id = it.next().ok_or(ArgParseError::MissingRelationshipId)?;

    let side = side
        .parse()
        .map_err(|_| ArgParseError::ParseRelationshipSide)?;
    let id: i64 = relationship_id
        .parse()
        .map_err(ArgParseError::ParseRelationshipId)?;

    Ok(Condition::NoRelationship(side, RelationshipId(id)))
}

struct Args {
    name: String,
    conditions: Vec<Condition>,
    filters: Vec<Condition>,
}

fn parse_args<It: Iterator<Item = String>>(
    mut it: It,
) -> Result<Args, ArgParseError> {
    let _program_name = it.next();

    let mut conditions = Vec::new();
    let mut filters = Vec::new();
    let mut name = None;

    while let Some(arg) = it.next() {
        match arg.as_ref() {
            "--name" => {
                name = it.next();
            }
            "--condition" => conditions.push(parse_filter(&mut it)?),
            "--filter" => filters.push(parse_filter(&mut it)?),
            "--help" => {
                help();
            }
            _ => return Err(ArgParseError::UnknownArg(arg)),
        }
    }

    let name = name.ok_or(ArgParseError::MissingFilterName)?;

    Ok(Args {
        name,
        conditions,
        filters,
    })
}

fn help() -> ! {
    let program_name = std::env::args()
        .next()
        .unwrap_or("create-root-filter".to_string());
    println!(
        "\
             Usage: {} [args]\n\
             \n\
             --name: Name for filter\n\
             --filter: Can be passed multiple times to combine filters (in order)\n\
             --condition: Can be passed multiple times to combine conditions (in order)\n\
             \n\
             Filter options:\n\
             no_relationship [side] [relationship_id]\n\
             \tShows elements that do not have a relationship where they are on the provided side\n\
             \tside: [dest, source]\n\
             has_relationship_with_variable_item [side] [relationship_id]\n\
             \tShows elements that have a relationship with the item associated with the filter from a specific side\n\
             \tside: [dest, source]\n\
             no_relationship_with_specific_item [item_id] [side] [relationship_id]\n\
             \tShows elements that have no relationship with a specific item from a specific side\n\
             \tside: [dest, source]\n\
             ",
        program_name
    );

    std::process::exit(1);
}

fn main() {
    let args = match parse_args(std::env::args()) {
        Ok(v) => v,
        Err(e) => {
            println!("{e}");
            help();
        }
    };
    let mut db = todo_fs::db::Db::new("test_db".into()).expect("failed to open db");
    db.add_item_filter(&args.name, &args.conditions, &args.filters).expect("failed to insert item filters");
    //let request = ClientRequest::CreateFilter(filter);
    //api::send_client_request(&request);
}
