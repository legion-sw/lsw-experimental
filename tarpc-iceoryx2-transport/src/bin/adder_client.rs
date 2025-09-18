use std::{env, process};

use tarpc::context;
use tarpc_iceoryx2_transport::{
    IceoryxConfig, IceoryxStream, Role, addition::AdderClient, bincode_transport,
};

type BoxError = Box<dyn std::error::Error + Send + Sync + 'static>;

fn usage(program: &str) {
    eprintln!("Usage: {program} <service-name> <x> <y> [iterations]");
}

fn into_box_error<E>(err: E) -> BoxError
where
    E: std::error::Error + Send + Sync + 'static,
{
    Box::new(err)
}

fn parse_i64(value: Option<String>, name: &str, program: &str) -> i64 {
    let raw = value.unwrap_or_else(|| {
        usage(program);
        process::exit(64);
    });

    raw.parse::<i64>().unwrap_or_else(|err| {
        eprintln!("Failed to parse {name} as i64: {err}");
        process::exit(65);
    })
}

fn parse_iterations(value: Option<String>) -> usize {
    match value {
        Some(raw) => raw.parse::<usize>().unwrap_or_else(|err| {
            eprintln!("Failed to parse iterations as usize: {err}");
            process::exit(65);
        }),
        None => 1,
    }
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), BoxError> {
    let mut args = env::args();
    let program = args.next().unwrap_or_else(|| "adder_client".to_string());

    let service_name = args.next().unwrap_or_else(|| {
        usage(&program);
        process::exit(64);
    });
    let x = parse_i64(args.next(), "x", &program);
    let y = parse_i64(args.next(), "y", &program);
    let iterations = parse_iterations(args.next());

    if args.next().is_some() {
        usage(&program);
        process::exit(64);
    }

    let stream = IceoryxStream::connect(&service_name, Role::Client, IceoryxConfig::default())
        .map_err(into_box_error)?;
    let transport = bincode_transport(stream);
    let client = AdderClient::new(Default::default(), transport).spawn();

    let mut last_result = 0i64;
    for _ in 0..iterations {
        last_result = client
            .add(context::current(), x, y)
            .await
            .map_err(into_box_error)?;
    }

    println!("{last_result}");

    Ok(())
}
