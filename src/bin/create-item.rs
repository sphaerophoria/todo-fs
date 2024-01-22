use todo_fs::fuse::api::{self, ClientRequest, ClientResponse, CreateItemRequest};

fn get_item_name_from_args<It: Iterator<Item = String>>(mut it: It) -> String {
    let program_name = it.next().expect("no program name provided");

    let mut item_name = None;
    for arg in it {
        if arg == "--help" {
            help(&program_name)
        }

        if item_name.is_some() {
            println!("Unexpected extra argument");
            help(&program_name);
        }

        item_name = Some(arg);
    }

    let Some(item_name) = item_name else {
        println!("Please provide item name");
        help(&program_name)
    };

    item_name
}

fn help(program_name: &str) -> ! {
    println!(
        "\
        Usage: {program_name} item_name\n\
    "
    );

    std::process::exit(1);
}

fn main() {
    let item_name = get_item_name_from_args(std::env::args());

    let request = ClientRequest::CreateItem(CreateItemRequest { name: item_name });
    let response = api::send_client_request(&request);
    let Some(ClientResponse::CreateItem(response)) = response else {
        panic!("Unexpected response");
    };

    println!("{}", response.path.display());
}
