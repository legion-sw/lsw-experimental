use std::{env, io, process};

use futures::StreamExt;
use tarpc::server::{BaseChannel, Channel};
use tarpc_iceoryx2_transport::{
    IceoryxConfig, IceoryxStream, Role,
    addition::{Adder, AdderService},
    bincode_transport,
};

fn usage(program: &str) {
    eprintln!("Usage: {program} <service-name>");
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> io::Result<()> {
    let mut args = env::args();
    let program = args.next().unwrap_or_else(|| "adder_server".to_string());
    let service_name = match args.next() {
        Some(name) => name,
        None => {
            usage(&program);
            process::exit(64);
        }
    };

    println!("Starting adder server on `{service_name}`");

    let stream = IceoryxStream::connect(&service_name, Role::Server, IceoryxConfig::default())?;
    let transport = bincode_transport(stream);

    BaseChannel::with_defaults(transport)
        .execute(AdderService.serve())
        .for_each(|fut| async move {
            tokio::spawn(fut);
        })
        .await;

    Ok(())
}
