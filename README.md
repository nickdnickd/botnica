
# Botnica - Transport files to Glacier and S3
Cut through the ice with Rust, delivering files to Glacier and s3 Asynhronously. Named after an Estonian [icebreaker](https://en.wikipedia.org/wiki/MSV_Botnica).

# Environments
These parameters must be defined somewhere in your environment or passed as parameters
```
AWS_ACCESS_KEY_ID
AWS_SECRET_ACCESS_KEY
```

## Development Environment

### Rust
Get by installing rustup and running it. [Here](https://www.rust-lang.org/tools/install) is the official site.

## Production Environment
`cargo build --release`


### Containerized builds and release
This is still experimental to test building in a conatiner vs. cross-compiling. The main Dockerfile here is for fedora