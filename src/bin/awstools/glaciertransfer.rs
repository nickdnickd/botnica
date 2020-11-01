use hex;
use bytes::{BytesMut, Bytes};
use futures::future::join_all;
use futures::stream::TryStreamExt;
use rusoto_core::{ByteStream, Region};
use rusoto_glacier::{
    AbortMultipartUploadInput, GlacierClient, InitiateMultipartUploadInput, UploadArchiveInput,
    UploadMultipartPartInput, CompleteMultipartUploadInput, Glacier
};
use sha2::{Digest, Sha256};
use slog::{debug, error, info, warn, Logger};
use sloggers::terminal::{Destination, TerminalLoggerBuilder};
use sloggers::types::{Severity, TimeZone};
use sloggers::Build;
use std::error::Error;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio;
use tokio::io::{AsyncReadExt, BufReader};
use tokio_util::codec::{BytesCodec, FramedRead};

use std::str::FromStr;

// TODO write more throttling tests.
// One for a file that is uploading too quickly
// one for a file that is uploading too slowly and taking up memory
#[tokio::test]
async fn test_throttling() {
    // Resource counter
    let completed_part_counter = Arc::new(Mutex::new(0i64));
    let async_completed_parts = Arc::clone(&completed_part_counter);

    // made up parts queued
    let parts_queued = 2i64;
    let part_size_bytes = 10i64;
    let memory_limit_bytes = 20i64;

    let mut builder = TerminalLoggerBuilder::new();
    builder.level(Severity::Debug);
    builder.destination(Destination::Stdout);
    builder.timezone(TimeZone::Utc);

    let logger = builder.build().unwrap();
    let logger_clone = logger.clone();

    // Simulate the upload for a single part
    info!(logger, "About to spawn async upload");
    let jhandle = tokio::spawn(async move {
        info!(logger_clone.to_owned(), "starting upload");
        tokio::time::delay_for(Duration::new(0, 500000000)).await;
        let mut data = async_completed_parts.lock().unwrap();
        *data += 1i64;
        info!(logger_clone.to_owned(), "upload complete");
    });

    info!(logger, "throttling function");
    let result_after = throttle(
        parts_queued,
        part_size_bytes,
        Arc::clone(&completed_part_counter),
        memory_limit_bytes,
        logger.to_owned(),
    )
    .await
    .unwrap();

    info!(logger, "throttling complete");

    let task_result = tokio::join!(jhandle);
    info!(logger, "delay task result: {:?}", task_result);

    assert!(result_after);
}

#[tokio::test]
async fn test_upload_speed() {
    let uploaded_bytes = 1024f64 * 1024f64; // 1 MB
    let duration_nano = 1_000_000_000f64; // 1s
    assert_eq!(calculate_upload_speed(uploaded_bytes, duration_nano), 8f64);

    // Ensure that durations less than zero do not cause divide by zero errors
    let short_duration: f64 = 1_000f64; // 1ms
    assert_eq!(
        calculate_upload_speed(uploaded_bytes, short_duration),
        8_000_000f64
    );
}

#[tokio::test]
async fn test_memory_held() {
    let part_size = 20 * 1024 * 1024; // 20 MB
    let parts_queued = 5;
    let parts_completed = 1;

    // We are holding on to 80 MB in memory
    assert_eq!(
        calculate_memory_held_bytes(parts_queued, part_size, parts_completed),
        80 * 1024 * 1024
    );
}

pub fn calculate_upload_speed(bytes_uploaded: f64, duration_ns: f64) -> f64 {
    // Calculates in megabytes per second per duration
    return (((bytes_uploaded * 8f64) / 1024f64) / 1024f64) / (duration_ns / 1_000_000_000f64);
}

pub fn calculate_memory_held_bytes(
    parts_queued: i64,
    part_size_bytes: i64,
    completed_parts: i64,
) -> i64 {
    // assumption: once the part goes out of scope, the memory is released

    (parts_queued - completed_parts) * part_size_bytes
}

pub async fn throttle(
    parts_queued: i64,
    part_size_bytes: i64,
    completed_part_counter: Arc<Mutex<i64>>,
    memory_limit_bytes: i64,
    logger: Logger,
) -> Result<bool, Box<dyn Error>> {
    let mut parts_clone: i64;
    let mut throttle_notify = false;

    loop {
        parts_clone = *completed_part_counter.lock().unwrap();
        let mem_held = calculate_memory_held_bytes(parts_queued, part_size_bytes, parts_clone);

        if mem_held < memory_limit_bytes {
            return Ok(true);
        }
        if !throttle_notify {
            debug!(
                logger,
                "Throttling: Memory held {:?}  Memory limit {:?}", mem_held, memory_limit_bytes
            );
            throttle_notify = true;
        }
        tokio::time::delay_for(Duration::new(0, 100_000_000)).await;
    }
}

pub async fn get_buf_sha256(bytes: &Vec<u8>) -> GenericArray {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hasher.finalize()
}

pub async fn glacier_put_file(
    glacier_client: GlacierClient,
    archive_name: String,
    vault_name: String,
    mut file: tokio::fs::File,
    file_len: u64,
) -> Result<String, Box<dyn Error>> {
    
    let buf: &mut Vec<u8> = &mut Vec::new();
    file.read_to_end(buf).await;

    let mut hasher = Sha256::new();
    hasher.update(buf);
    let checksum = hex::encode(hasher.finalize());

    let immut_buf: Vec<u8> = buf.to_vec();

    let result = glacier_client
        .upload_archive(UploadArchiveInput {
            account_id: "-".to_string(),
            body: Some(Bytes::from(immut_buf)),
            checksum: Some(checksum),
            archive_description: Some(archive_name),
            vault_name: vault_name,
            ..Default::default()
        })
        .await?;

    println!("Upload result {:?}", result);

    Ok(result.checksum.unwrap())
}

pub async fn glacier_upload_part(
    glacier_client: GlacierClient,
    part_buffer: Vec<u8>,
    vault_name: String,
    archive_name: String,
    upload_id: String,
    part_number: i64,
    completed_part_count: Arc<Mutex<i64>>,
    checksum: Option<String>,
    bytes_range: Option<String>,
    logger: Logger,
) -> Result<String, ()> {
    info!(
        logger.to_owned(),
        "uploading part {} buffer length: {:?}",
        part_number,
        part_buffer.len()
    );
    
    let upload_part = UploadMultipartPartInput {
        account_id: "-".to_string(),
        body: Some(part_buffer.to_owned().into()),
        checksum: checksum.to_owned(),
        range: bytes_range,
        upload_id: upload_id.to_owned(),
        vault_name: vault_name.to_owned(),
        ..Default::default()
    };

    let result = glacier_client.upload_multipart_part(upload_part.into()).await;
    let response = match result {
        Ok(upload_part) => upload_part,
        Err(rus_err) => {
            warn!(
                logger.to_owned(),
                "Part {:?} had error {:?}, retrying",
                part_number.to_owned(),
                rus_err
            );

            let retry_upload_part = UploadMultipartPartInput {
                account_id: "-".to_string(),
                body: Some(part_buffer.to_owned().into()),
                checksum: checksum.to_owned(),
                range: bytes_range,
                upload_id: upload_id.to_owned(),
                vault_name: vault_name.to_owned(),
                ..Default::default()
            };
            glacier_client.upload_multipart_part(retry_upload_part).await.unwrap() // panic if the second attempt fails
        }
    };

    match response.checksum.clone() {
        Some(checksum) => {
            let mut counter = completed_part_count.lock().unwrap();
            *counter += 1;
            info!(
                logger.to_owned(),
                "Successful upload of part {:?} with etag: {:?}", part_number, checksum
            );
        }
        None => {
            error!(
                logger.to_owned(),
                "ERROR: Upload part {} FAILED. Valid response but no checksum generated",
                part_number
            );
        }
    }

    Ok(checksum.unwrap())
}

pub async fn glacier_multipart_upload_file(
    glacier_client: GlacierClient,
    archive_name: String,
    vault_name: String,
    file: tokio::fs::File,
    file_len: u64,
    logger: Logger,
) -> Result<String, Box<dyn Error>> {
    const SEND_THRESHOLD: usize = 20 * 1024 * 1024;
    const HELD_MEMORY_LIMIT: i64 = 512 * 1024 * 1024; // 512 MB

    let mut buffer = BytesMut::new();

    // create another buffer that will be used to send
    let mut part_buffer = BytesMut::with_capacity(SEND_THRESHOLD);

    // todo calculate part size

    let mpu = glacier_client
        .initiate_multipart_upload(InitiateMultipartUploadInput {
            account_id: "-".to_string(),
            archive_description: Some(archive_name),
            part_size: Some(String::from(SEND_THRESHOLD)),
            vault_name: vault_name.to_owned(),
            ..Default::default()
        })
        .await?;

    let mut part_number = 1i64;
    let completed_part_count: Arc<Mutex<i64>> = Arc::new(Mutex::new(0i64));

    let mut sent_bytes = 0u64;
    let mut part_upload_futures = Vec::new();
    let mut buf_reader = BufReader::new(file);

    loop {
        let bytes_read = buf_reader.read_buf(&mut buffer).await?;

        part_buffer.extend_from_slice(&buffer[..bytes_read]);
        buffer.clear();

        if part_buffer.len() >= SEND_THRESHOLD || bytes_read == 0 {
            let checksum = get_buf_sha256(part_buffer);

            debug!(
                logger.to_owned(),
                "filled part_buffer with {:?} bytes, number {:?} with checksum {:?}",
                part_buffer.len(),
                part_number,
                checksum.to_owned()
            );

            let body_copy = part_buffer.to_vec();
            let glacier_client_clone = glacier_client.clone();
            let vault_name_clone = vault_name.clone();
            let object_key_clone = object_key.clone();
            let upload_id_clone = mpu.upload_id.clone().unwrap().clone();

            debug!(logger.to_owned(), "body_copy len {:?}", body_copy.len());

            let check_completed_part = Arc::clone(&completed_part_count);
            throttle(
                part_number,
                SEND_THRESHOLD as i64,
                check_completed_part,
                HELD_MEMORY_LIMIT,
                logger.to_owned(),
            )
            .await?;

            let completed_part_counter = Arc::clone(&completed_part_count);
            let logger_clone = logger.clone();

            let bytes_range: String =
                format!("{}-{}", sent_bytes, sent_bytes + (part_buffer.len() as u64) - 1u64);

            part_upload_futures.push(tokio::spawn(async move {
                glacier_upload_part(
                    glacier_client_clone,
                    body_copy,
                    vault_name_clone,
                    object_key_clone,
                    upload_id_clone,
                    part_number.to_owned(),
                    completed_part_counter,
                    Some(checksum),
                    bytes_range,
                    logger_clone,
                )
                .await
                .unwrap()
            }));

            sent_bytes += part_buffer.len() as u64;
            part_buffer.clear();
            part_number = part_number + 1i64;
        }

        if bytes_read == 0 {
            break;
        }
    } // loop over reading bytes from a file

    let completed_parts = join_all(part_upload_futures).await;

    let check_completed_part = Arc::clone(&completed_part_count);
    if (part_number - 1) == *check_completed_part.lock().unwrap() {
        info!(logger, "Completed parts matches sent parts")
    } else {
        // abort the multipart upload
        warn!(logger,
            "part numbers to not match: {:?} sent and {:?} measured as complete. aborting multi part upload",
            part_number,
            *check_completed_part.lock().unwrap()
        );
        glacier_client
            .abort_multipart_upload(
                AbortMultipartUploadInput {
                    account_id: "-".to_string(),
                    vault_name: vault_name.to_owned(),
                    upload_id: mpu.upload_id.unwrap().to_owned(),
                    ..Default::default()
                }
                .into(),
            )
            .await?;

        return Err("Aborted the multipart upload. Completed parts do not match sent parts".into());
    }

    if sent_bytes == file_len {
        info!(logger, "Measured file length matches sent bytes {}", sent_bytes);
    } else {
        warn!(logger, "WARNING: File length {} does NOT match sent bytes {}", file_len, sent_bytes);
    }

    

    // TODO calculate overall checksum, also need to explore how to read exact amounts from the file

    let complete_result = glacier_client
        .complete_multipart_upload(CompleteMultipartUploadInput {
            upload_id: upload_id,
            account_id: "-".to_string(),
            archive_size: file_len,
            checksum: ,
            upload_id: upload_id,
            vault_name: vault_name
            ..Default::default()
        })
        .await?;

    Ok(complete_result.e_tag.unwrap())
}

pub async fn run(
    vault_name: String,
    archive_name: String,
    region: Option<String>,
    filepath: String,
    logger: Logger,
) -> Result<(), Box<dyn Error>> {
    const MAX_GLACIER_PUT_SIZE: u64 = 100 * 1024 * 1024;

    let glacier_region = match region {
        Some(region) => Region::from_str(&*region)?,
        None => Region::default(),
    };

    let glacier_client = GlacierClient::new(glacier_region);

    let file = tokio::fs::File::open(filepath.clone()).await?;
    let file_len = std::fs::metadata(filepath)?.len();

    let e_tag: String;

    let now = Instant::now(); // TODO timing and average speed calculation later

    if file_len > MAX_GLACIER_PUT_SIZE {
        info!(
            logger,
            "File is over {:?} Using multipart upload", MAX_GLACIER_PUT_SIZE
        );
        e_tag = glacier_multipart_upload_file(
            glacier_client,
            archive_name,
            vault_name,
            file,
            file_len,
            logger.to_owned(),
        )
        .await?;
    } else {
        e_tag = glacier_put_file(glacier_client, archive_name, vault_name, file, file_len).await?;
    }

    let elapsed_ms = now.elapsed().as_millis();
    let upload_speed =
        calculate_upload_speed(file_len as f64, (elapsed_ms as f64) * 1024f64 * 1024f64);
    info!(logger, "Upload time: {:?} seconds", elapsed_ms / 1000);
    info!(logger, "Upload speed {:?} Mbps", upload_speed);

    info!(logger, "Overall e_tag: {:?}", e_tag);

    Ok(())
}
