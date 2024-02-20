use thiserror::Error;
use todo_fs::fuse::api::{self, ClientRequest, ClientResponse, CreateRelationshipRequest};

#[derive(Error, Debug)]
enum ArgParseError {
    #[error("no from name provided")]
    NoFromNameProvided,
    #[error("no to name provided")]
    NoToNameProvided,
    #[error("unhandled argument: {0}")]
    UnhandledArg(String),
}

fn parse_args<It: Iterator<Item = String>>(mut it: It) -> CreateRelationshipRequest {
    let program_name = it
        .next()
        .unwrap_or_else(|| "create-item-relationship".to_string());

    let res = (|| -> Result<CreateRelationshipRequest, ArgParseError> {
        let mut from_name = None;
        let mut to_name = None;
        while let Some(arg) = it.next() {
            match arg.as_ref() {
                "--from" => {
                    from_name = it.next();
                }
                "--to" => {
                    to_name = it.next();
                }
                "--help" => {
                    help(&program_name);
                }
                s => return Err(ArgParseError::UnhandledArg(s.to_string())),
            }
        }

        let from_name = from_name.ok_or(ArgParseError::NoFromNameProvided)?;

        let to_name = to_name.ok_or(ArgParseError::NoToNameProvided)?;

        Ok(CreateRelationshipRequest { from_name, to_name })
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
        --from <from name>\n\
        --to <to name>\n"
    );

    std::process::exit(1);
}

fn main() {
    let request = parse_args(std::env::args());

    let request = ClientRequest::CreateRelationship(request);
    let response = api::send_client_request(&request);
    let Some(ClientResponse::CreateRelationship(response)) = response else {
        panic!("Unexpected response");
    };

    println!("{}", response.path.display());
}
