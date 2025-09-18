use std::sync::atomic::{AtomicUsize, Ordering};

use criterion::{Criterion, criterion_group, criterion_main};
use futures::StreamExt;
use tarpc::{
    context,
    server::{BaseChannel, Channel},
};
use tarpc_iceoryx2_transport::{IceoryxConfig, IceoryxStream, Role, bincode_transport};

#[tarpc::service]
pub trait Arithmetic {
    async fn add(x: i32, y: i32) -> i32;
}

#[derive(Clone)]
struct ArithmeticImpl;

impl Arithmetic for ArithmeticImpl {
    async fn add(self, _: context::Context, x: i32, y: i32) -> i32 {
        x + y
    }
}

fn unique_service_name() -> String {
    static COUNTER: AtomicUsize = AtomicUsize::new(0);
    let id = COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("tarpc/bench/{}/{}", std::process::id(), id)
}

fn roundtrip_benchmark(c: &mut Criterion) {
    let runtime = tokio::runtime::Runtime::new().expect("tokio runtime");

    c.bench_function("tarpc_roundtrip", |b| {
        b.to_async(&runtime).iter(|| async {
            let base = unique_service_name();

            let server_stream =
                IceoryxStream::connect(&base, Role::Server, IceoryxConfig::default()).unwrap();
            let server_transport = bincode_transport(server_stream);
            let server = tokio::spawn(async move {
                BaseChannel::with_defaults(server_transport)
                    .execute(ArithmeticImpl.serve())
                    .for_each(|fut| async move {
                        tokio::spawn(fut);
                    })
                    .await;
            });

            let client_stream =
                IceoryxStream::connect(&base, Role::Client, IceoryxConfig::default()).unwrap();
            let transport = bincode_transport(client_stream);
            let client = ArithmeticClient::new(Default::default(), transport).spawn();

            let _ = client
                .add(context::current(), 1, 2)
                .await
                .expect("rpc call succeeds");

            drop(client);
            server.abort();
        });
    });
}

criterion_group!(benches, roundtrip_benchmark);
criterion_main!(benches);
