use serde::{Deserialize, Serialize};
use std::{
    fs::{File, OpenOptions},
    io::{Read, Write},
    path::PathBuf,
};

pub const API_HANDLE_PATH: &str = "/.api_handle";

/// Assuming the binary that is calling this is mapped to /bin/, we can resolve the file handle by
/// going up a dir and looking at the API_HANDLE_PATH
fn open_api_handle_for_file() -> Result<File, std::io::Error> {
    let current_exe = std::env::current_exe()?;
    let fs_root = current_exe
        .parent()
        .expect("exe should have a parent")
        .parent()
        .expect("exe is expected to be in /bin/");
    let socket_path = fs_root.join(&API_HANDLE_PATH[1..]);
    OpenOptions::new()
        .read(true)
        .write(true)
        .create(false)
        .open(socket_path)
}

pub fn send_client_request(request: &ClientRequest) -> Option<ClientResponse> {
    let serialized = serde_json::to_vec(&request).expect("failed to serialize request");

    let mut api_handle = open_api_handle_for_file().expect("failed to open api handle");

    api_handle
        .write_all(&serialized)
        .expect("failed to write request");

    let mut response_buf = vec![0; 4096];

    let num_bytes_read = api_handle
        .read(&mut response_buf)
        .expect("failed to read response");

    match request {
        ClientRequest::CreateItemRelationship(_) => return None,
        ClientRequest::CreateItem(_) | ClientRequest::CreateRelationship(_) => (),
    }

    let response: ClientResponse =
        serde_json::from_slice(&response_buf[0..num_bytes_read]).expect("failed to parse response");

    Some(response)
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "snake_case")]
pub struct CreateItemRequest {
    pub name: String,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "snake_case")]
pub struct CreateItemResponse {
    pub path: PathBuf,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "snake_case")]
pub struct CreateRelationshipRequest {
    pub from_name: String,
    pub to_name: String,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "snake_case")]
pub struct CreateRelationshipResponse {
    pub path: PathBuf,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "snake_case")]
pub struct CreateItemRelationshipRequest {
    pub relationship_id: i64,
    pub from_id: i64,
    pub to_id: i64,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "type", content = "data")]
#[serde(rename_all = "snake_case")]
pub enum ClientRequest {
    CreateItem(CreateItemRequest),
    CreateRelationship(CreateRelationshipRequest),
    CreateItemRelationship(CreateItemRelationshipRequest),
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "type", content = "data")]
#[serde(rename_all = "snake_case")]
pub enum ClientResponse {
    CreateItem(CreateItemResponse),
    CreateRelationship(CreateRelationshipResponse),
}
