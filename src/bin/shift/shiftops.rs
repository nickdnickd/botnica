#![allow(non_snake_case)]
use async_trait::async_trait;
use bytes::BytesMut;
use chrono::{DateTime, NaiveDateTime, Utc};
use futures::TryStreamExt;
use http::StatusCode;
use reqwest;
use rusoto_core::Region;
use rusoto_credential::{
    AutoRefreshingProvider, AwsCredentials, CredentialsError, ProfileProvider,
    ProvideAwsCredentials, StaticProvider,
};
use rusoto_s3::{GetObjectRequest, S3Client, S3};
use serde::{Deserialize, Serialize};
use slog::{debug, info, warn, Logger};
use std::collections::HashMap;
use std::env;
use std::error::Error;
use std::fs;
use std::path::Path;
use std::time::Instant;
use tokio;

use std::sync::{Arc, Mutex};

#[path = "../awstools/s3transfer.rs"]
pub mod s3transfer;

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone, Default)]
pub struct ShiftConnection {
    app_key: String,
    api_url: String,
    api_username: String,
    api_password: String,
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone, Default)]
pub struct ShiftUploadConfig {
    project_id: String,
    project_name: String,
    host_context: String,
    optional_folder_id: Option<String>,
    optional_folder_name: Option<String>,
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub struct ShiftProject {
    id: String,
    name: String,
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone, Default)]
pub struct MPUploadParameters {
    fileName: String,
    assetUrl: String,
    expiration: i64,
    accessKey: String,
    secretKey: String,
    sessionId: String,
    sessionToken: String,
    objectKey: String,
    bucketName: String,
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone, Default)]
pub struct ShiftConfig {
    shift_api: ShiftConnection,
    shift_upload_configs: HashMap<String, ShiftUploadConfig>,
}

//#[derive(Default)]
pub struct MSCredentialProvider {
    shift_config: ShiftConfig,
    host_context: String,
    mediasilo_session_key: String,
    file_name: String,
    mp_upload_params: Arc<Mutex<Option<MPUploadParameters>>>,
    logger: Logger,
}

#[async_trait]
impl ProvideAwsCredentials for MSCredentialProvider {
    async fn credentials(&self) -> Result<AwsCredentials, CredentialsError> {
        let file_name = self.file_name.to_owned();

        let multipart_request_ticket = self.mp_upload_params.lock().unwrap().clone();

        let creds = match multipart_request_ticket {
            Some(valid_ticket) => {
                request_upload_ticket(
                    &self.shift_config,
                    self.mediasilo_session_key.to_owned(),
                    self.host_context.to_owned(),
                    file_name,
                    Some(valid_ticket.sessionId.to_owned()),
                )
                .await
            }
            None => {
                request_upload_ticket(
                    &self.shift_config,
                    self.mediasilo_session_key.to_owned(),
                    self.host_context.to_owned(),
                    file_name,
                    None,
                )
                .await
            }
        };

        match creds {
            Ok(valid_creds) => {
                info!(self.logger, "Refresh success");

                let mut new_mediasilo_ticket = self.mp_upload_params.lock().unwrap();
                *new_mediasilo_ticket = Some(valid_creds.clone());

                // Return the properly formatted AwsCredentials to the Autorefresher

                let expire_dt = DateTime::<Utc>::from_utc(
                    NaiveDateTime::from_timestamp(valid_creds.expiration / 1000, 0),
                    Utc,
                );

                Ok(AwsCredentials::new(
                    valid_creds.accessKey,
                    valid_creds.secretKey,
                    Some(valid_creds.sessionToken),
                    Some(expire_dt),
                ))
            }
            Err(msg) => Err(CredentialsError::new(format!(
                "Unable to retreive credentials from mediasilo: {:?}",
                msg
            ))),
        }
    }
}

pub fn shift_bucket_to_region(
    shift_bucket_name: String,
    logger: Logger,
) -> Result<Region, Box<dyn Error>> {
    if shift_bucket_name.eq("ingest-east-shift") {
        return Ok(Region::UsEast1);
    }

    let warn_message = String::from(
        "No existing mapping for mediasilo bucket name: ".to_string() + &shift_bucket_name.clone(),
    );

    warn!(logger, "Defaulting to UsEast1: {:?}", warn_message);

    Ok(Region::UsEast1)
}

pub fn get_parent_folder(filepath: String, parent_folder: Option<String>) -> String {
    let path = Path::new(&filepath);
    match parent_folder {
        Some(folder) => {
            // remove leading slash of parent folder if present
            folder
                .trim_start_matches("/")
                .to_string()
                .trim_end_matches("/")
                .to_string()
        }
        None => path
            .parent()
            .unwrap()
            .file_stem()
            .unwrap()
            .clone()
            .to_os_string()
            .into_string()
            .unwrap(),
    }
}

pub async fn load_s3_configuration_string(
    config_file: String,
    s3_keys: Option<(String, String)>,
    test: bool,
    logger: Logger,
) -> Result<String, &'static str> {
    let obj_key = if test {
        "config/test/shift-upload.yml".to_string()
    } else {
        "config/prod/shift-upload.yml".to_string()
    };

    let s3_client: S3Client = match s3_keys {
        Some(keys) => {
            let (access_key, secret_key) = keys;
            let trint_creds = StaticProvider::new(access_key, secret_key, None, None);

            let s3_client = S3Client::new_with(
                rusoto_core::request::HttpClient::new().expect("Failed to create HTTP client"),
                trint_creds,
                Region::UsEast1,
            );
            s3_client
        }
        None => {
            let mut trint_profile = ProfileProvider::new().unwrap();
            trint_profile.set_profile("shift");
            let s3_client = S3Client::new_with(
                rusoto_core::request::HttpClient::new().expect("Failed to create HTTP client"),
                trint_profile,
                Region::UsEast1,
            );
            s3_client
        }
    };

    let get_req = GetObjectRequest {
        bucket: config_file.to_owned(),
        key: obj_key.to_owned(),
        ..Default::default()
    };

    info!(logger, "setting up s3 client to read configuration");

    let result = s3_client
        .get_object(get_req)
        .await
        .expect("config cannot be found. please check if is a valid file or bucket");

    let stream = result.body.unwrap();
    let config_str = stream
        .map_ok(|b| BytesMut::from(&b[..]))
        .try_concat()
        .await
        .unwrap();
    match String::from_utf8((&config_str).to_vec()) {
        Ok(return_string) => Ok(return_string),
        Err(_) => Err("unable to read the character buffer from the bucket"),
    }
}

pub async fn load_configuration(
    config_file: String,
    s3_keys: Option<(String, String)>,
    test: bool,
    logger: Logger,
) -> Result<ShiftConfig, serde_yaml::Error> {
    let contents = match fs::read_to_string(config_file.clone()) {
        Ok(result) => result,
        Err(_) => {
            info!(
                logger,
                "config file doesn't exist on the local filesystem at {:?}.", config_file
            );
            info!(
                logger,
                "attempting to read from this location as an s3 bucket."
            );
            let s3_string =
                load_s3_configuration_string(config_file, s3_keys, test, logger.to_owned()).await;
            s3_string.unwrap()
        }
    };

    if contents.is_empty() {
        warn!(
            logger,
            "Upload configurations have been disabled! Config contents are present but empty"
        );
    }
    let trint_config: ShiftConfig = serde_yaml::from_str(&contents)?;
    Ok(trint_config)
}

pub fn get_host_context(
    config: &ShiftConfig,
    watchfolder_name: String,
) -> Result<String, Box<dyn Error>> {
    if config.shift_upload_configs.contains_key(&watchfolder_name) {
        return Ok(config.shift_upload_configs[&watchfolder_name]
            .host_context
            .to_owned());
    } else {
        Err(format!(
            "Cannot find watchfolder/config name {:?} in the configuration file",
            watchfolder_name
        )
        .into())
    }
}

/// Creats the session. Either returns a session token or throws an error
async fn create_mediasilo_session(
    config: &ShiftConfig,
    host_context: String,
    logger: Logger,
) -> Result<String, Box<dyn Error>> {
    let client = reqwest::Client::new();

    let url = reqwest::Url::parse(&config.shift_api.api_url)?.join("session/")?;

    debug!(logger, "Using url {:?}", url);

    let mut map = HashMap::new();
    map.insert("accountName", host_context.to_owned());
    map.insert("userName", config.shift_api.api_username.clone());
    map.insert("password", config.shift_api.api_password.clone());

    let res: serde_json::Value =
        serde_json::from_str(&client.post(url).json(&map).send().await?.text().await?)?;

    debug!(logger, "Result session id {:?}", res["id"].clone());

    Ok(res["id"].to_string().replace("\"", ""))
}

pub async fn get_all_projects(
    config: &ShiftConfig,
    session_key: String,
    host_context: String,
    logger: Logger,
) -> Result<Vec<ShiftProject>, Box<dyn Error>> {
    let client = reqwest::Client::new();
    let url = reqwest::Url::parse(&config.shift_api.api_url)?.join("projects/")?;

    debug!(
        logger,
        "getting all projects with session key: {}",
        session_key.clone()
    );
    info!(
        logger,
        "getting all projects with host context: {}",
        host_context.to_owned()
    );

    let res = client
        .get(url)
        .header("MediaSiloSessionKey", session_key.clone())
        .header("MediaSiloHostContext", host_context.to_owned())
        .send()
        .await?;

    debug!(logger, "Mediasilo request response {:?}", res);

    let projects: Result<Vec<ShiftProject>, Box<dyn Error>> = match res.error_for_status() {
        Ok(_res) => {
            let result_text = _res.text().await?;
            debug!(logger, "Project result text: {:?}", result_text);
            let prjs: Vec<ShiftProject> = serde_json::from_str(&result_text)?;
            debug!(logger, "Projects found: {:?}", prjs.to_owned());
            Ok(prjs)
        }
        Err(err) => {
            let err_str = err.to_string();
            debug!(logger, "Project request error: {:?}", err_str);
            Err(err_str)?
        }
    };

    projects
}

async fn request_upload_ticket(
    config: &ShiftConfig,
    session_key: String,
    host_context: String,
    file_name: String,
    session_id: Option<String>,
) -> Result<MPUploadParameters, Box<dyn Error>> {
    let client = reqwest::Client::new();

    let url = reqwest::Url::parse(&config.shift_api.api_url)?.join("assets/multipart/upload/")?;

    let mut map = HashMap::new();
    map.insert("fileName", file_name);
    // The session id is used to refresh the credentials if it exists
    // See https://docs.mediasilo.com/docs/multipart-upload
    match session_id {
        Some(sid) => map.insert("sessionId", sid),
        None => None,
    };

    let res = client
        .post(url)
        .header("MediaSiloSessionKey", session_key.clone())
        .header("MediaSiloHostContext", host_context.to_owned())
        .json(&map)
        .send()
        .await?
        .text()
        .await?;

    let multipart_upload_parameters: MPUploadParameters = serde_json::from_str(&res)?;
    Ok(multipart_upload_parameters)
}

async fn create_ms_asset(
    config: &ShiftConfig,
    session_key: String,
    host_context: String,
    asset_url: String,
    project_id: String,
    target_folder_id: Option<String>,
    logger: Logger,
) -> Result<String, Box<dyn Error>> {
    let client = reqwest::Client::new();
    let url = reqwest::Url::parse(&config.shift_api.api_url)?.join("assets/")?;

    let mut asset_creation_payload = HashMap::new();
    asset_creation_payload.insert("sourceUrl", asset_url);
    asset_creation_payload.insert("projectId", project_id);

    match target_folder_id {
        Some(folder_id) => {
            info!(logger, "Uploading to folder id: {:?}", folder_id.to_owned());
            asset_creation_payload.insert("folderId", folder_id);
        }
        None => {}
    }

    let res = client
        .post(url)
        .header("MediaSiloSessionKey", session_key.clone())
        .header("MediaSiloHostContext", host_context.to_owned())
        .json(&asset_creation_payload)
        .send()
        .await?
        .text()
        .await?;

    info!(logger, "create_ms_asset result: {:?}", res.clone());
    // TODO check the return value to have that inform the process rc
    Ok(res)
}

async fn upload_to_ms_s3(
    shift_config: &ShiftConfig,
    session_key: String,
    host_context: String,
    filepath: String,
    name: Option<String>,
    logger: Logger,
) -> Result<String, Box<dyn Error>> {
    let file_path = Path::new(&filepath);

    let upload_as_name = match name {
        Some(declared_name) => declared_name,
        None => file_path.file_name().unwrap().to_str().unwrap().to_string(),
    };

    info!(logger, "Uploading as {:?}", upload_as_name.to_owned());

    let refresh_provider = AutoRefreshingProvider::new(MSCredentialProvider {
        shift_config: shift_config.clone(),
        host_context: host_context.to_owned(),
        mediasilo_session_key: session_key.to_owned(),
        file_name: upload_as_name.to_owned(),
        mp_upload_params: Arc::new(Mutex::new(None)),
        logger: logger.to_owned(),
    })?;

    debug!(
        logger,
        "AWS Credentials refresh configured. Now awaiting the first refresh"
    );
    refresh_provider.credentials().await?; // Trigger the refresh so we can harvest the variables

    let multipart_request_ticket = refresh_provider
        .get_ref()
        .mp_upload_params
        .lock()
        .unwrap()
        .clone()
        .unwrap();
    let object_key = multipart_request_ticket.objectKey.to_owned();
    let bucket_name = multipart_request_ticket.bucketName.to_owned();
    let asset_url = multipart_request_ticket.assetUrl.to_owned();

    let s3_client = S3Client::new_with(
        rusoto_core::request::HttpClient::new().expect("Failed to create HTTP client"),
        refresh_provider,
        shift_bucket_to_region(bucket_name.to_owned(), logger.to_owned())?,
    );

    // Determine weather to do a simple put, or to multipart upload.
    // According to aws docs https://docs.aws.amazon.com/AmazonS3/latest/dev/UploadingObjects.html
    // A single put request can occur for files under 5 GB in size,
    // but the multipart section reccomends using MPU after 100 MB
    // https://docs.aws.amazon.com/AmazonS3/latest/dev/uploadobjusingmpu.html
    const MAX_S3_PUT_SIZE: u64 = 100 * 1024 * 1024;

    let file = tokio::fs::File::open(filepath.clone()).await?;
    let file_len = std::fs::metadata(filepath)?.len();

    let e_tag: String;

    let now = Instant::now();

    if file_len > MAX_S3_PUT_SIZE {
        info!(
            logger,
            "File is over {:?} Using multipart upload", MAX_S3_PUT_SIZE
        );
        e_tag = s3transfer::s3_multipart_upload_file(
            s3_client,
            object_key,
            bucket_name,
            file,
            file_len,
            logger.to_owned(),
        )
        .await?;
    } else {
        e_tag = s3transfer::s3_put_file(s3_client, object_key, bucket_name, file, file_len).await?;
    }
    let elapsed_ms = now.elapsed().as_millis();
    let upload_speed = s3transfer::calculate_upload_speed(
        file_len as f64,
        (elapsed_ms as f64) * 1024f64 * 1024f64,
    );
    info!(
        logger,
        "Upload Completed. Time: {:?} seconds",
        elapsed_ms / 1000
    );
    info!(logger, "Upload speed {:?} Mbps", upload_speed);
    debug!(logger, "s3 upload result {:?}", e_tag); // To deem if success, use ETag

    Ok(asset_url)
}

pub async fn upload_to_mediasilo(
    config: &ShiftConfig,
    session_key: String,
    file_name: String,
    host_context: String,
    name: Option<String>,
    project_id: String,
    target_folder_id: Option<String>,
    logger: Logger,
) -> Result<(), Box<dyn Error>> {
    let asset_url = upload_to_ms_s3(
        config,
        session_key.to_owned(),
        host_context.to_owned(),
        file_name,
        name,
        logger.to_owned(),
    )
    .await?;

    create_ms_asset(
        config,
        session_key,
        host_context.to_owned(),
        asset_url,
        project_id,
        target_folder_id,
        logger.to_owned(),
    )
    .await?;

    Ok(())
}

pub fn create_bucket_config(
    config_access_key_id: Option<String>,
    config_access_key_secret: Option<String>,
    logger: Logger,
) -> Option<(String, String)> {
    match config_access_key_id {
        Some(access_key_id) => {
            // require there to be a secret
            match config_access_key_secret {
                Some(access_key_secret) => Some((access_key_id, access_key_secret)),
                None => {
                    warn!(logger, "Access key provided but not secret, will not use this key for s3 auth. Please specify both");
                    None
                }
            }
        }
        None => {
            info!(logger, "attempt to read the environment for bucket config");

            match env::var("SHIFT_UPLOAD_CONFIG_ACCESS_KEY_ID") {
                Ok(access_key_id) => match env::var("SHIFT_UPLOAD_CONFIG_SECRET_ACCESS_KEY") {
                    Ok(secret_access_key) => {
                        info!(
                            logger,
                            "shift/mediasilo upload config keys found in the environment"
                        );
                        Some((access_key_id, secret_access_key))
                    }
                    Err(_) => {
                        info!(logger, "No shift/mediasilo config secret access key present in environment: SHIFT_UPLOAD_CONFIG_SECRET_ACCESS_KEY");
                        None
                    }
                },
                Err(_) => {
                    info!(logger, "No trint config access key id present in environment: SHIFT_UPLOAD_CONFIG_ACCESS_KEY_ID");
                    None
                }
            }
        }
    }
}

pub async fn override_project_with_username(
    project_id: String,
    username: Option<String>,
    config: &ShiftConfig,
    session_key: String,
    host_context: String,
    logger: Logger,
) -> Result<String, Box<dyn Error>> {
    match username {
        Some(un) => {
            info!(
                logger,
                "Username {:?} supplied. Searching for matching projects",
                un.to_owned()
            );
            let projects: Vec<ShiftProject> = get_all_projects(
                &config.to_owned(),
                session_key.to_owned(),
                host_context.to_owned(),
                logger.to_owned(),
            )
            .await?;
            let mut found_project: Option<String> = None;
            for shift_project in projects {
                if shift_project.name.to_lowercase() == un.to_lowercase() {
                    info!(
                        logger,
                        "Found matching project {:?} with id {:?}",
                        shift_project.name,
                        shift_project.id
                    );
                    found_project = Some(shift_project.id);
                }
            }
            match found_project {
                Some(found_project_id) => Ok(found_project_id),
                None => Err(format!("Could not find the project for username {:?}", un))?,
            }
        }
        None => Ok(project_id),
    }
}

pub async fn run(
    config: String,
    filepath: String,
    name: Option<String>,
    project_id: Option<String>,
    parent_folder: Option<String>,
    username: Option<String>,
    s3_keys: Option<(String, String)>,
    test: bool,
    logger: Logger,
) -> Result<(), Box<dyn Error>> {
    let shift_config = load_configuration(config, s3_keys, test, logger.to_owned()).await;

    match shift_config {
        Ok(config) => {
            info!(logger, "Shift config parsed.");

            let watchfolder_name = get_parent_folder(filepath.to_owned(), parent_folder);
            let host_context = get_host_context(&config, watchfolder_name.to_owned())?;

            let ms_session =
                create_mediasilo_session(&config, host_context.to_owned(), logger.to_owned())
                    .await?;

            let mut final_project_id: String = match project_id {
                Some(prj_id) => prj_id,
                None => config.shift_upload_configs[&watchfolder_name]
                    .project_id
                    .to_owned(),
            };

            final_project_id = override_project_with_username(
                final_project_id,
                username.to_owned(),
                &config,
                ms_session.to_owned(),
                host_context.to_owned(),
                logger.to_owned(),
            )
            .await?;
            info!(logger, "Using Project ID {:?}", final_project_id);

            let target_folder_id = match username {
                Some(un) => {
                    info!(
                        logger,
                        "Ignoring folder id option. Reason: username argument {:?} supplied", un
                    );
                    None
                }
                None => config.shift_upload_configs[&watchfolder_name]
                    .optional_folder_id
                    .to_owned(),
            };

            upload_to_mediasilo(
                &config,
                ms_session,
                filepath,
                host_context,
                name,
                final_project_id,
                target_folder_id,
                logger,
            )
            .await?;
            Ok(())
        }
        Err(e) => Err(Box::new(e)),
    }
}
