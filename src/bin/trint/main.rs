use clap::Clap;
use std::process;

use slog::{error, info};
use sloggers::terminal::{Destination, TerminalLoggerBuilder};
use sloggers::types::{Severity, TimeZone};
use sloggers::Build;

mod trintops;

/// Command line utility to upload a file to Trint
/// given a path to the file.
#[derive(Clap)]
#[clap(version = "0.3.0", author = "Nick D.")]
struct Opts {
    /// Config, either point to a file on the local filesystem or let the
    /// program read from a known s3 location by default. This requires
    /// either the profile to be present on the users home folder or keys
    #[clap(short = 'c' as char, long = "config", default_value = "wcprodsystems-trint")]
    config: String,
    /// Config access key, option to provide the access key to our configuration
    #[clap(short = 'i' as char, long = "config_access_key_id")]
    config_access_key_id: Option<String>,
    ///  Config access secret, option to provide the access key to our configuration
    #[clap(short = 's' as char, long = "config_access_key_secret")]
    config_access_key_secret: Option<String>,
    /// Filepath to the target directory
    filepath: String,
    /// Parent folder, option to give the name explicitly
    #[clap(short = 'p' as char, long = "parent_folder")]
    parent_folder: Option<String>,
    /// Name, optional naming of the file in trint
    #[clap(short = 'n' as char, long = "name")]
    name: Option<String>,
    /// If we are loading from s3, use the test configuration
    #[clap(short = 't' as char, long = "test")]
    test: bool,
}

fn main() {
    let opts: Opts = Opts::parse();

    let mut builder = TerminalLoggerBuilder::new();
    builder.level(Severity::Debug);
    builder.destination(Destination::Stdout);
    builder.timezone(TimeZone::Utc);

    let logger = builder.build().unwrap();

    // Gets a value for config if supplied by user, or defaults to "default.conf"
    info!(logger, "Configuration file location: {}", opts.config);
    info!(logger, "Using input file: {}", opts.filepath);

    let credential_tuple = trintops::create_bucket_config(
        opts.config_access_key_id,
        opts.config_access_key_secret,
        logger.to_owned(),
    );

    match credential_tuple {
        Some(_) => info!(logger, "bucket keys found"),
        None => info!(
            logger,
            "no bucket keys found, will check user aws trint profile"
        ),
    }

    if let Err(e) = trintops::run(
        opts.config,
        opts.filepath,
        opts.name,
        opts.parent_folder,
        credential_tuple,
        opts.test,
        logger.to_owned(),
    ) {
        error!(logger, "Application error: {}", e);

        process::exit(1);
    }
}
