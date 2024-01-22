use todo_fs::fuse::api::{self, ClientRequest, CreateItemRelationshipRequest};

use thiserror::Error;

#[derive(Error, Debug)]
enum ArgParseError {
    #[error("no relationship provided")]
    NoRelationshipProvided,
    #[error("no from id provided")]
    NoFromIdProvided,
    #[error("no to id provided")]
    NoToIdProvided,
    #[error("failed to parse relationship id")]
    ParseRelationshipId(#[source] std::num::ParseIntError),
    #[error("failed to parse from id")]
    ParseFromId(#[source] std::num::ParseIntError),
    #[error("failed to parse to id")]
    ParseToId(#[source] std::num::ParseIntError),
    #[error("unhandled argument: {0}")]
    UnhandledArg(String),
}

fn parse_args<It: Iterator<Item = String>>(mut it: It) -> CreateItemRelationshipRequest {
    let program_name = it
        .next()
        .unwrap_or_else(|| "create-item-relationship".to_string());

    let res = (|| -> Result<CreateItemRelationshipRequest, ArgParseError> {
        let mut relationship_id = None;
        let mut from_id = None;
        let mut to_id = None;
        while let Some(arg) = it.next() {
            match arg.as_ref() {
                "--relationship" => {
                    relationship_id = it.next().map(|x| x.parse::<i64>());
                }
                "--from" => {
                    from_id = it.next().map(|x| x.parse::<i64>());
                }
                "--to" => {
                    to_id = it.next().map(|x| x.parse::<i64>());
                }
                "--help" => {
                    help(&program_name);
                }
                s => return Err(ArgParseError::UnhandledArg(s.to_string())),
            }
        }

        let relationship_id = relationship_id
            .ok_or(ArgParseError::NoRelationshipProvided)?
            .map_err(ArgParseError::ParseRelationshipId)?;

        let from_id = from_id
            .ok_or(ArgParseError::NoFromIdProvided)?
            .map_err(ArgParseError::ParseFromId)?;

        let to_id = to_id
            .ok_or(ArgParseError::NoToIdProvided)?
            .map_err(ArgParseError::ParseToId)?;

        Ok(CreateItemRelationshipRequest {
            relationship_id,
            from_id,
            to_id,
        })
    })();

    match res {
        Ok(v) => v,
        Err(e) => {
            println!("{e}");
            help(&program_name);
        }
    }
}

fn help(program_name: &str) -> ! {
    println!(
        "\
        Usage: {program_name} [args]\n\
        \n\
        Args:\n\
        --relationship <relationship id>\n\
        --from <item id>\n\
        --to <item id>\n"
    );

    std::process::exit(1);
}

fn main() {
    let request = parse_args(std::env::args());

    let request = ClientRequest::CreateItemRelationship(request);
    api::send_client_request(&request);
}
