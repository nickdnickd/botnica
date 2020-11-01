use slog::error;
use sloggers::terminal::{Destination, TerminalLoggerBuilder};
use sloggers::types::{Severity, TimeZone};
use sloggers::Build;

use clap::Clap;
use std::process;
use tokio;

mod s3transfer;
// todo add glacier back in when ready
// mod glaciertransfer;

/// Command line utility to upload a file to S3 compatible storage
/// given a path to the file and a name
#[derive(Clap)]
#[clap(version = "0.4.0", author = "Nick D.")]
struct Opts {
    /// S3 Bucket name
    #[clap(short = 'b' as char, long = "bucket")]
    bucket: String,
    /// Object Key, the key inside the buc
    #[clap(short = 'k' as char, long = "object_key")]
    object_key: String,
    /// region, option to define the region
    #[clap(short = 'p' as char, long = "region")]
    region: Option<String>,
    /// Filepath to the file to be uploaded
    filepath: String,
    // /// glacier
    // #[clap(subcommand)]
    // glacier: Option<SubCommand>,
}

// #[derive(Clap)]
// enum SubCommand {
//     #[clap(version = "0.4.0", author = "Nick D.")]
//     Glacier(Glacier),
// }

// /// A subcommand for specifying a glacier upload
// #[derive(Clap)]
// struct Glacier {
//     #[clap(short = 'b' as char, long = "bucket")]
//     vault: String,
//     /// Object Key, the key inside the buc
//     #[clap(short = 'k' as char, long = "object_key")]
//     archive: Option<String>,
//     /// region, option to define the region
//     #[clap(short = 'p' as char, long = "region")]
//     region: Option<String>,
//     /// Filepath to the file to be uploaded
//     filepath: String,
// }

#[tokio::main]
async fn main() -> () {
    let opts: Opts = Opts::parse();

    let mut builder = TerminalLoggerBuilder::new();
    builder.level(Severity::Debug);
    builder.destination(Destination::Stdout);
    builder.timezone(TimeZone::Utc);

    let logger = builder.build().unwrap();

    if let Err(e) = s3transfer::run(
        opts.bucket,
        opts.object_key,
        opts.region,
        opts.filepath,
        logger.to_owned(),
    )
    .await
    {
        error!(logger, "Application error: {}", e);

        process::exit(1);
    }
}
