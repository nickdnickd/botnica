#![allow(non_snake_case)]
extern crate chrono_tz;
use base64;
use bytes::BytesMut;
use http::StatusCode;
use md5;
use reqwest;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Number, Value};
use slog::{debug, info, warn, Logger};
use std::collections::HashMap;
use std::error::Error;
use std::fs;
use std::path::Path;
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, BufReader};

use chrono::{DateTime, FixedOffset, Local, NaiveDateTime, TimeZone, Utc};
use chrono_tz::US::Pacific;
use std::str::FromStr;

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone, Default)]
pub struct PixApi {
    app_key: String,
    api_url: String,
    api_username: String,
    api_password: String,
    upload_machine_uuid: String,
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone, Default)]
pub struct PixUploadConfig {
    project_id: String,
    optional_project_name: Option<String>,
    folder_id: String,
    optional_folder_name: Option<String>,
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone, Default)]
pub struct PixConfig {
    pix_api: PixApi,
    pix_upload_configs: HashMap<String, PixUploadConfig>,
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone, Default)]
pub struct PixPermissions {
    upload: bool,
    send: String,
    project_access_expiration: Option<String>,
    contacts: bool,
    can_create_users: bool,
    can_process_requests: bool,
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone, Default)]
pub struct PixProject {
    id: String,
    name: String,
    default_upload_folder: Option<String>,
    root_folder: String,
    permissions: PixPermissions,
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone, Default)]
pub struct PixUser {
    id: String,
    name: String,
    username: String,
    email: String,
    super_user: bool,
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone, Default)]
pub struct PixSession {
    session_id: String,
    ttl: u64,
    active_user: PixUser,
    active_project: PixProject,
}

#[derive(Debug, Clone, Default)]
pub struct PixConnection {
    pix_session: PixSession,
    pix_client: reqwest::Client,
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone, Default)]
pub struct PixTransfer {
    status: String,
    percentage_transferred: String,
    transfer_rate: String,
    eta: String,
    priority: String,
    start_time: String,
    last_contact_time: String,
    chunk_size: String,
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone, Default)]
pub struct PixUploadTicket {
    id: String,
    transfer: PixTransfer,
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone, Default)]
pub struct PixError {
    number: i64,
    r#type: String,
    user_message: String,
}

const DEFAULT_CHUNK_SIZE: u64 = 20 * 1024 * 1024;

pub async fn load_configuration(
    config_file: String,
    test: bool,
    logger: Logger,
) -> Result<PixConfig, Box<dyn Error>> {
    let contents = match fs::read_to_string(config_file.clone()) {
        Ok(result) => result,
        Err(e) => {
            return Err(e.into());
        }
    };

    if contents.is_empty() {
        warn!(
            logger,
            "Upload configurations have been disabled! Config contents are present but empty"
        );
    }
    let pix_config: PixConfig = serde_yaml::from_str(&contents)?;
    Ok(pix_config)
}

pub async fn get_root_folders(
    config: PixConfig,
    connection: PixConnection,
    logger: Logger,
) -> Result<(), Box<dyn Error>> {
    let url = reqwest::Url::parse(&config.pix_api.api_url)?.join("folders/root/")?;

    let response = connection
        .pix_client
        .get(url)
        .header("X-PIX-App-Key", config.pix_api.app_key.to_owned())
        .send()
        .await?;

    info!(logger, "Folder response, {:?}", response);
    Ok(())
}

/// Log in and select the project
pub async fn login_to_pix(
    config: PixConfig,
    config_key: String,
    logger: Logger,
) -> Result<PixConnection, Box<dyn Error>> {
    let client: reqwest::Client = reqwest::ClientBuilder::new().cookie_store(true).build()?;

    let url = reqwest::Url::parse(&config.pix_api.api_url)?.join("session/")?;

    info!(
        logger,
        "Using url {:?} username: {:?} password {:?}",
        url,
        config.pix_api.api_username.to_owned(),
        config.pix_api.api_password.to_owned()
    );

    let mut map = HashMap::new();
    map.insert(
        "active_project",
        config.pix_upload_configs[&config_key].project_id.to_owned(),
    );
    map.insert("username", config.pix_api.api_username.to_owned());
    map.insert(
        "password",
        String::from_utf8(base64::decode(config.pix_api.api_password.to_owned())?)?,
    );

    let response = client
        .put(url)
        .header("X-PIX-App-Key", config.pix_api.app_key.to_owned())
        .json(&map)
        .send()
        .await?;

    info!(logger, "resonse headers {:?}", response);

    info!(logger, "Response Cookie: {:?}", response.cookies().next());
    let response_payload = response.text().await?;

    info!(logger, "Response String: {:?}", response_payload);

    let res: PixSession = serde_json::from_str(&response_payload)?;

    info!(
        logger,
        "Result session id {:?} and ttl {:?}",
        res.session_id.to_owned(),
        res.ttl
    );

    Ok(PixConnection {
        pix_client: client,
        pix_session: res,
    })
}

pub async fn get_datetime_in_pst() -> Result<String, Box<dyn Error>> {
    // Pix natively uses US Pacific Time
    let now = Local::now();
    let now_pt = now.with_timezone(&Pacific);

    Ok(now_pt.format("%Y-%m-%d %H:%M:%S").to_string())
}

pub async fn register_upload_chunked(
    config: PixConfig,
    connection: PixConnection,
    file_name: String,
    folder_id: String,
    file_len: u64,
    md5sum: Option<String>,
    logger: Logger,
) -> Result<PixUploadTicket, Box<dyn Error>> {
    let mut upload_params = Map::new();
    upload_params.insert("filename".to_string(), Value::String(file_name));
    upload_params.insert("folder_id".to_string(), Value::String(folder_id));
    upload_params.insert(
        "host_uuid".to_string(),
        Value::String(config.pix_api.upload_machine_uuid),
    );
    upload_params.insert("size".to_string(), Value::Number(Number::from(file_len)));
    match md5sum {
        Some(checksum) => {
            upload_params.insert("checksum".to_string(), Value::String(checksum));
        }
        None => {}
    };

    upload_params.insert(
        "m_stamp".to_string(),
        Value::String(get_datetime_in_pst().await?),
    );

    let mut payload = Map::new();
    payload.insert("source".to_string(), Value::Object(upload_params));

    let payload_str = serde_json::to_string(&payload)?;

    debug!(logger, "Request input payload: {:?}", payload_str);

    let url = reqwest::Url::parse(&config.pix_api.api_url)?.join("uploads")?;

    let response = connection
        .pix_client
        .post(url)
        .header("Content-type", "application/json;charset=utf-8")
        .header("X-PIX-App-Key", config.pix_api.app_key.to_owned())
        .body(payload_str)
        .send()
        .await?;

    if response.status().is_success() {
        info!(logger, "registered upload response: , {:?}", response);
        let upload_ticket: PixUploadTicket = serde_json::from_str(&response.text().await?)?;
        return Ok(upload_ticket);
    } else {
        Err("Unable to register upload to pix".into())
    }
}

pub async fn read_n(
    reader: &mut BufReader<tokio::fs::File>,
    bytes_to_read: u64,
) -> Result<Vec<u8>, Box<dyn Error>> {
    let mut buf = vec![];
    let mut chunk = reader.take(bytes_to_read);
    let n = chunk.read_to_end(&mut buf).await.unwrap();
    assert_eq!(bytes_to_read as usize, n);
    Ok(buf)
}

pub async fn compute_md5(
    filepath: &Path,
    file_len: u64,
    logger: Logger,
) -> Result<String, Box<dyn Error>> {
    info!(
        logger,
        "computing the checksum of {:?} with length {:?}", filepath, file_len
    );
    let file = tokio::fs::File::open(filepath.to_owned()).await?;

    let CHUNK_SIZE: u64 = 40 * 1024 * 1024;
    let mut reader = BufReader::new(file);

    let mut context = md5::Context::new();

    let mut bytes_left = file_len;
    let mut chunk_size: u64;
    loop {
        if bytes_left >= CHUNK_SIZE {
            chunk_size = CHUNK_SIZE;
        } else {
            chunk_size = bytes_left;
        };
        debug!(
            logger,
            "file_size {:?}, bytes_left {:?}, chunk_size {:?}", file_len, bytes_left, chunk_size
        );
        context.consume(read_n(&mut reader, chunk_size).await?);

        bytes_left = bytes_left - chunk_size;
        if bytes_left == 0 {
            break;
        };
    }

    let digest = context.compute();

    Ok(format!("{:x}", digest))
}

pub async fn pix_upload_part(
    config: PixConfig,
    connection: PixConnection,
    registered_upload: PixUploadTicket,
    data: Vec<u8>,
    start_byte: u64,
    end_byte: u64,
    logger: Logger,
) -> Result<reqwest::Response, Box<dyn Error>> {
    let path = format!(
        "uploads/{}/bytes/{}-{}",
        registered_upload.id.to_owned(),
        start_byte,
        end_byte
    );
    let url = reqwest::Url::parse(&config.pix_api.api_url)?.join(&path)?;

    info!(logger, "Uploading part: {:?}", url);

    let response = connection
        .pix_client
        .put(url)
        .header("Content-type", "application/json;charset=utf-8")
        .header("X-PIX-App-Key", config.pix_api.app_key.to_owned())
        .body(data)
        .send()
        .await?;

    if response.status().is_success() {
        info!(
            logger,
            "Upload {:?} Part {}-{} upload success", registered_upload.id, start_byte, end_byte
        );
        Ok(response)
    } else {
        let response_text = response.text().await?;
        debug!(logger, "Response Text {:?}", response_text);
        Err(response_text.into())
    }
}

pub async fn get_multipart_pix_upload_status(
    config: PixConfig,
    connection: PixConnection,
    registered_upload: PixUploadTicket,
    logger: Logger,
) -> Result<String, Box<dyn Error>> {
    let path = format!("uploads/{}", registered_upload.id);
    let url = reqwest::Url::parse(&config.pix_api.api_url)?.join(&path)?;

    info!(logger, "Uploading part: {:?}", url);

    let response = connection
        .pix_client
        .get(url)
        .header("Content-type", "application/json;charset=utf-8")
        .header("X-PIX-App-Key", config.pix_api.app_key.to_owned())
        .send()
        .await?;

    debug!(logger, "Response {:?}", response);

    if response.status().is_success() {
        let ul_ticket: PixUploadTicket = serde_json::from_str(&response.text().await?)?;

        Ok(ul_ticket.transfer.status)
    } else {
        let response_text = response.text().await?;
        debug!(logger, "Response Text {:?}", response_text);
        Err(response_text.into())
    }
}

pub async fn multipart_pix_upload(
    config: PixConfig,
    connection: PixConnection,
    registered_upload: PixUploadTicket,
    filepath: &Path,
    file_len: u64,
    logger: Logger,
) -> Result<(), Box<dyn Error>> {
    info!(logger, "Multi-Part Uploading {:?}", filepath);
    let file = tokio::fs::File::open(filepath.to_owned()).await?;

    let mut chunk_size: u64 = match registered_upload.transfer.chunk_size.parse::<u64>() {
        Ok(parsed_chunk_size) => parsed_chunk_size,
        Err(_) => DEFAULT_CHUNK_SIZE,
    };
    info!(logger, "Using chunk size {}", chunk_size);
    let mut reader = BufReader::new(file);

    let mut bytes_left = file_len;

    loop {
        if bytes_left >= chunk_size {
            chunk_size = chunk_size;
        } else {
            chunk_size = bytes_left;
        };
        debug!(
            logger,
            "file_size {:?}, bytes_left {:?}, chunk_size {:?}", file_len, bytes_left, chunk_size
        );

        let start_byte = file_len - bytes_left;
        let end_byte = start_byte + chunk_size - 1;

        let response = pix_upload_part(
            config.to_owned(),
            connection.to_owned(),
            registered_upload.to_owned(),
            read_n(&mut reader, chunk_size).await?,
            start_byte,
            end_byte,
            logger.to_owned(),
        )
        .await?;

        if !response.status().is_success() {
            return Err(format!("Upload part failed").into());
        }

        bytes_left = bytes_left - chunk_size;
        if bytes_left == 0 {
            break;
        };
    }

    // TODO run a get request to ensure 100 percent upload success
    let status: String = get_multipart_pix_upload_status(
        config.to_owned(),
        connection.to_owned(),
        registered_upload.to_owned(),
        logger.to_owned(),
    )
    .await?;

    if status.to_lowercase() == "finished".to_string().to_lowercase() {
        info!(
            logger,
            "Upload {} successfully completed", registered_upload.id
        );
        return Ok(());
    }

    Err("multipart upload success validation not implemented".into())
}

pub async fn upload_to_pix(
    config: PixConfig,
    connection: PixConnection,
    filename: String,
    filepath: &Path,
    folder_id: String,
    logger: Logger,
) -> Result<(), Box<dyn Error>> {
    println!("inside upload_to_pix");
    let file_len = std::fs::metadata(&filepath)?.len();
    let md5sum = compute_md5(filepath, file_len, logger.to_owned()).await?;

    info!(
        logger,
        "Registering upload from file: {:?} to folder: {:?} size (bytes): {:?}  md5sum:{:?}",
        &filepath,
        folder_id,
        file_len,
        md5sum
    );

    let registered_upload = register_upload_chunked(
        config.to_owned(),
        connection.to_owned(),
        filename,
        folder_id,
        file_len,
        Some(md5sum),
        logger.to_owned(),
    )
    .await?;

    multipart_pix_upload(
        config,
        connection,
        registered_upload,
        filepath,
        file_len,
        logger,
    )
    .await
}

pub async fn run(
    config: String,
    filepath: String,
    name: Option<String>,
    config_key: String,
    test: bool,
    logger: Logger,
) -> Result<(), Box<dyn Error>> {
    let pix_config = load_configuration(config, test, logger.to_owned()).await;

    let path = Path::new(&filepath);

    let filename = match name {
        Some(declared_name) => declared_name,
        None => path.file_name().unwrap().to_str().unwrap().to_string(),
    };

    match pix_config {
        Ok(config) => {
            info!(logger, "Pix config parsed. {:?}", config);

            let pix_connection =
                login_to_pix(config.to_owned(), config_key.to_owned(), logger.to_owned()).await?;

            info!(
                logger,
                "Successfully logged into pix with connection params {:?}", pix_connection
            );

            // get_root_folders(
            //     config.to_owned(),
            //     pix_connection.to_owned(),
            //     logger.to_owned(),
            // )
            // .await?;

            // TODO determine if we need folder

            upload_to_pix(
                config.to_owned(),
                pix_connection,
                filename,
                path,
                config.pix_upload_configs[&config_key].folder_id.to_owned(),
                logger,
            )
            .await?;

            Ok(())
        }
        Err(e) => Err(e),
    }
}
