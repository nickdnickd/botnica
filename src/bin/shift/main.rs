use clap::Clap;
use std::process;
use tokio;

use slog::{error, info, Logger};
use sloggers::terminal::{Destination, TerminalLoggerBuilder};
use sloggers::types::{Severity, TimeZone};
use sloggers::Build;

mod shiftops;

/// Command line utility to upload a file to Shift (or Mediasilo)
/// given a path to the file.
#[derive(Clap)]
#[clap(version = "0.3.0", author = "Nick D.")]
struct Opts {
    /// Config, either point to a file on the local filesystem or let the
    /// program read from a known s3 location by default. This requires
    /// either the profile to be present on the users home folder or keys
    #[clap(short = 'c', long = "config", default_value = "wcprodsystems-shiftops")]
    config: String,
    /// Config access key, option to provide the access key to our configuration
    #[clap(short = 'i' as char, long = "config_access_key_id")]
    config_access_key_id: Option<String>,
    ///  Config access secret, option to provide the access key to our configuration
    #[clap(short = 's' as char, long = "config_access_key_secret")]
    config_access_key_secret: Option<String>,
    /// Parent folder, option to give the name explicitly
    #[clap(short = 'f', long = "parent_folder")]
    parent_folder: Option<String>,
    /// Username, if specified upload to a MS project of the same name OVERRIDES Project
    #[clap(short = 'u', long = "username")]
    username: Option<String>,
    /// Filepath to the file to be uploaded
    filepath: String,
    /// Project ID, option to give the name explicitly
    #[clap(short = 'p' as char, long = "project_id")]
    project_id: Option<String>,
    /// Name, optional naming of the file in shift
    #[clap(short = 'n' as char, long = "name")]
    name: Option<String>,
    /// If we are loading from s3, use the test configuration
    #[clap(short = 't' as char, long = "test")]
    test: bool,
}

fn build_logger() -> Logger {
    // TODO experiment with leaving out severity
    let mut builder = TerminalLoggerBuilder::new();
    //builder.level(Severity::Debug);
    builder.destination(Destination::Stdout);
    builder.timezone(TimeZone::Utc);

    let logger = builder.build().unwrap();
    logger
}

#[tokio::main]
async fn main() -> () {
    let opts: Opts = Opts::parse();

    let logger = build_logger();

    info!(logger, "Configuration file location: {}", opts.config);
    info!(logger, "Using input file: {} and username {:?}", opts.filepath, opts.username);

    let credential_tuple = shiftops::create_bucket_config(
        opts.config_access_key_id,
        opts.config_access_key_secret,
        logger.to_owned(),
    );

    match credential_tuple {
        Some(_) => info!(logger, "bucket keys found"),
        None => info!(
            logger,
            "no AWS credentials found in environment, will check user AWS Shift profile"
        ),
    }

    if let Err(e) = shiftops::run(
        opts.config,
        opts.filepath,
        opts.name,
        opts.project_id,
        opts.parent_folder,
        opts.username,
        credential_tuple,
        opts.test,
        logger.to_owned(),
    )
    .await
    {
        error!(logger, "Application error: {}", e);

        process::exit(1);
    }
}
