use todo_fs::{
    db::{Condition, RelationshipId},
    fuse::api::{self, ClientRequest, CreateFilterRequest},
};

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

// FIXME: Duplicated
fn parse_filter<It: Iterator<Item = String>>(it: &mut It) -> Result<Condition, ArgParseError> {
    let filter_name = it.next().ok_or(ArgParseError::MissingFilterType)?;
    if filter_name != "no_relationship" {
        return Err(ArgParseError::UnknownFilter(filter_name));
    }

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
             \tside: [dest, source]\
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
