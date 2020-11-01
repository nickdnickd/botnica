use clap::Clap;
use std::process;
use tokio;

use slog::{error, info, Logger};
use sloggers::terminal::{Destination, TerminalLoggerBuilder};
use sloggers::types::{Severity, TimeZone};
use sloggers::Build;

mod pixops;

/// Command line utility to upload a file to PIX
/// given a path to the file.
#[derive(Clap)]
#[clap(version = "0.3.0", author = "Nick D.")]
struct Opts {
    /// Config, either point to a file on the local filesystem or let the
    /// program read from a known s3 location by default. This requires
    /// either the profile to be present on the users home folder or keys
    #[clap(short = 'c', long = "config", default_value = "wcprodsystems-pixops")]
    config: String,
    /// Parent folder, option to give the name explicitly
    #[clap(short = 'k', long = "config_key")]
    config_key : String,
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
    let mut builder = TerminalLoggerBuilder::new();
    // builder.level(Severity::Debug);
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
    info!(logger, "Using input file: {} and upload name {:?}", opts.filepath, opts.name);

    if let Err(e) = pixops::run(
        opts.config,
        opts.filepath,
        opts.name,
        opts.config_key,
        opts.test,
        logger.to_owned(),
    )
    .await
    {
        error!(logger, "Application error: {}", e);

        process::exit(1);
    }
}
