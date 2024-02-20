use todo_fs::fuse::api::{self, ClientRequest, DeleteItemRequest};

fn get_item_id_from_args<It: Iterator<Item = String>>(mut it: It) -> i64 {
    let program_name = it.next().expect("no program name provided");

    let mut item_id = None;
    for arg in it {
        if arg == "--help" {
            help(&program_name)
        }

        if item_id.is_some() {
            println!("Unexpected extra argument");
            help(&program_name);
        }

        item_id = Some(arg);
    }

    let Some(item_id) = item_id else {
        println!("Please provide item name");
        help(&program_name)
    };

    match item_id.parse() {
        Ok(v) => v,
        Err(e) => {
            println!("Failed to parse item id: {e}");
            help(&program_name);
        }
    }
}

fn help(program_name: &str) -> ! {
    println!(
        "\
        Usage: {program_name} item_id\n\
    "
    );

    std::process::exit(1);
}

fn main() {
    let item_id = get_item_id_from_args(std::env::args());

    let request = ClientRequest::DeleteItem(DeleteItemRequest { id: item_id });
    api::send_client_request(&request);
}
