extern crate yaml_rust;

use bytes::BytesMut;
use futures::TryStreamExt;
use rusoto_core::Region;
use rusoto_credential::{ProfileProvider, StaticProvider};
use rusoto_s3::{GetObjectRequest, S3Client, S3};
use serde::{Deserialize, Serialize};
use slog::{info, warn, Logger};
use std::collections::HashMap;
use std::env;
use std::error::Error;
use std::fs;
use std::path::Path;
use std::time::Duration;
use tokio::runtime::Runtime;

#[derive(Debug, PartialEq, Serialize, Deserialize)]
struct BucketConfig {
    access_key_id: String,
    secret_access_key: String,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
struct TrintConnection {
    key: String,
    api_url: String,
    upload_url: String,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct TrintConfig {
    trint_api: TrintConnection,
    trint_upload: HashMap<String, HashMap<String, String>>,
}

#[cfg(test)]
mod tests {
    use super::*;

    use sloggers::terminal::{Destination, TerminalLoggerBuilder};
    use sloggers::types::{Severity, TimeZone};
    use sloggers::Build;

    #[test]
    /// test that we maintain the following priority in config
    /// command line -> sidecar -> profile
    fn config_priority() {
        let mut builder = TerminalLoggerBuilder::new();
        builder.level(Severity::Debug);
        builder.destination(Destination::Stdout);
        builder.timezone(TimeZone::Utc);

        let logger = builder.build().unwrap();

        // Given
        let access_key: String = String::from("test_key");
        let key_secret: String = String::from("test_secret");

        assert_eq!(
            create_bucket_config(Some(access_key.clone()), None, logger.to_owned()),
            None
        );
        assert_eq!(
            create_bucket_config(
                Some(access_key.clone()),
                Some(key_secret.clone()),
                logger.to_owned()
            ),
            Some((access_key.clone(), key_secret.clone()))
        );

        env::set_var("TRINT_UPLOAD_CONFIG_ACCESS_KEY_ID", access_key.clone());
        env::set_var("TRINT_UPLOAD_CONFIG_SECRET_ACCESS_KEY", key_secret.clone());
        assert_eq!(
            create_bucket_config(None, None, logger),
            Some((access_key.clone(), key_secret.clone()))
        );
    }

    #[test]
    /// given a yaml config with optional webhook params,
    /// parse it to a single metadata string
    fn yaml_parse() {
        let config_string = "
        trint_api:
            key: testkey123
            upload_url: https://upload-url.com
            api_url: https://api_url.com
        trint_upload:
                tests:
                    workspace_name: Testing
                    workspace_id: ws1
                    folder_name: tests
                    folder_id: folder1
                    webhook_slack: https://thisisawebhook/4321
                    webhook_teams: https://thisisawebhook/1234
        ";

        let trint_config: TrintConfig = serde_yaml::from_str(&config_string).unwrap();

        assert_eq!(
            trint_config.trint_upload["tests"]["webhook_teams"],
            "https://thisisawebhook/1234"
        );
        assert_eq!(
            trint_config.trint_upload["tests"]["webhook_slack"],
            "https://thisisawebhook/4321"
        );
    }
}

pub fn trint_config_to_upload_metadata(
    trint_ws_config: &HashMap<String, String>,
) -> Result<String, serde_json::error::Error> {
    serde_json::to_string(&trint_ws_config.clone())
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

            match env::var("TRINT_UPLOAD_CONFIG_ACCESS_KEY_ID") {
                Ok(access_key_id) => match env::var("TRINT_UPLOAD_CONFIG_SECRET_ACCESS_KEY") {
                    Ok(secret_access_key) => {
                        info!(logger, "trint upload config keys found in the environment");
                        Some((access_key_id, secret_access_key))
                    }
                    Err(_) => {
                        info!(logger, "No trint config secret access key present in environment: TRINT_UPLOAD_CONFIG_SECRET_ACCESS_KEY");
                        None
                    }
                },
                Err(_) => {
                    info!(logger, "No trint config access key id present in environment: TRINT_UPLOAD_CONFIG_ACCESS_KEY_ID");
                    None
                }
            }
        }
    }
}

pub async fn load_s3_configuration_string(
    config_file: String,
    s3_keys: Option<(String, String)>,
    test: bool,
    logger: Logger,
) -> Result<String, &'static str> {
    let obj_key = if test {
        "config/test/trint-upload.yml".to_string()
    } else {
        "config/prod/trint-upload.yml".to_string()
    };

    let s3_client = match s3_keys {
        Some(keys) => {
            let (access_key, secret_key) = keys;
            let trint_creds = StaticProvider::new(access_key, secret_key, None, None);
            S3Client::new_with(
                rusoto_core::request::HttpClient::new().expect("Failed to create HTTP client"),
                trint_creds,
                Region::UsWest2,
            )
        }
        None => {
            let mut trint_profile = ProfileProvider::new().unwrap();
            trint_profile.set_profile("trint");
            S3Client::new_with(
                rusoto_core::request::HttpClient::new().expect("Failed to create HTTP client"),
                trint_profile,
                Region::UsWest2,
            )
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

pub fn load_configuration(
    config_file: String,
    s3_keys: Option<(String, String)>,
    test: bool,
    logger: Logger,
) -> Result<TrintConfig, serde_yaml::Error> {
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
            let s3_string = Runtime::new()
                .unwrap()
                .block_on(load_s3_configuration_string(
                    config_file,
                    s3_keys,
                    test,
                    logger.to_owned(),
                ));
            s3_string.unwrap()
        }
    };

    let trint_config: TrintConfig = serde_yaml::from_str(&contents)?;
    Ok(trint_config)
}

pub fn upload_request(
    config: TrintConfig,
    filepath: String,
    parent_folder: Option<String>,
    name: Option<String>,
    logger: Logger,
) -> Result<String, Box<dyn Error>> {
    info!(logger, "Parsing filepath {}", filepath);

    let path = Path::new(&filepath);
    let parent_string: String = match parent_folder {
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
    };
    let parent = &parent_string;
    info!(logger, "Parent folder name: {:?}", parent);

    if config.trint_upload.contains_key(parent) {
        info!(
            logger,
            "Parent folder found in config with workspace name {:?} and folder name {:?}",
            config.trint_upload[parent]["workspace_name"],
            config.trint_upload[parent]["folder_name"]
        );

        let workspace_id: String = config.trint_upload[parent]["workspace_id"].clone();
        let folder_id: String = config.trint_upload[parent]["folder_id"].clone();
        let filename = match name {
            Some(declared_name) => declared_name,
            None => path.file_name().unwrap().to_str().unwrap().to_string(),
        };

        let client = reqwest::blocking::Client::new();
        let file = fs::File::open(&filepath)?;

        let url = reqwest::Url::parse_with_params(
            &config.trint_api.upload_url,
            &[("workspace-id", workspace_id), ("folder-id", folder_id)],
        )?;

        info!(logger, "Using url {:?}", url);

        let timeout = Duration::new(60 * 60, 0);

        info!(logger, "Using timeout of {:?}", timeout);

        let res = client
            .post(url)
            .header("api-key", config.trint_api.key)
            .header("filename", filename)
            .header("Content-Length", fs::metadata(path)?.len())
            .header("Content-Type", "application/x-www-form-urlencoded")
            .header(
                "metadata",
                trint_config_to_upload_metadata(&config.trint_upload[parent])?,
            )
            .body(file)
            .timeout(timeout)
            .send();

        Ok(res?.text()?)
    } else {
        Err("Parent folder name not found in the configuration".into())
    }
}

pub fn run(
    config: String,
    filepath: String,
    name: Option<String>,
    parent_folder: Option<String>,
    s3_keys: Option<(String, String)>,
    test: bool,
    logger: Logger,
) -> Result<(), Box<dyn Error>> {
    let trint_config = load_configuration(config, s3_keys, test, logger.to_owned());

    match trint_config {
        Ok(v) => {
            info!(logger, "Trint config parsed.");

            match upload_request(v, filepath, parent_folder, name, logger.to_owned()) {
                Ok(result) => {
                    info!(logger, "result {:?}", result);
                    Ok(())
                }
                Err(e) => Err(e),
            }
        }
        Err(e) => Err(Box::new(e)),
    }
}
