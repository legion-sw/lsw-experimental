use std::{
    convert::TryInto,
    sync::atomic::{AtomicUsize, Ordering},
};

use criterion::{Criterion, criterion_group, criterion_main};
use futures::StreamExt;
use rkyv::{from_bytes, to_bytes};
use speedy::{LittleEndian, Readable, Writable};
use tarpc::{
    Transport, context,
    server::{BaseChannel, Channel},
};
use tarpc_iceoryx2_transport::{
    IceoryxConfig, IceoryxStream, Role, bincode_transport, bitcode_transport, postcard_transport,
};

#[tarpc::service(derive = [
    ::tarpc::serde::Serialize,
    ::tarpc::serde::Deserialize,
])]
pub trait Arithmetic {
    async fn add(x: i32, y: i32) -> i32;
    async fn process_frame(frame: Vec<u8>) -> i32;
}

#[derive(Clone)]
struct ArithmeticImpl;

impl Arithmetic for ArithmeticImpl {
    async fn add(self, _: context::Context, x: i32, y: i32) -> i32 {
        x + y
    }

    async fn process_frame(self, _: context::Context, frame: Vec<u8>) -> i32 {
        frame.len().try_into().expect("frame length fits in i32")
    }
}

fn unique_service_name() -> String {
    static COUNTER: AtomicUsize = AtomicUsize::new(0);
    let id = COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("tarpc/bench/{}/{}", std::process::id(), id)
}

type ServerSink = tarpc::Response<ArithmeticResponse>;
type ServerItem = tarpc::ClientMessage<ArithmeticRequest>;
type ClientSink = tarpc::ClientMessage<ArithmeticRequest>;
type ClientItem = tarpc::Response<ArithmeticResponse>;

fn bench_roundtrip<ServerTransport, ClientTransport, MakeServer, MakeClient>(
    c: &mut Criterion,
    name: &str,
    make_server_transport: MakeServer,
    make_client_transport: MakeClient,
) where
    ServerTransport: Transport<ServerSink, ServerItem> + Send + 'static,
    ClientTransport: Transport<ClientSink, ClientItem> + Send + 'static,
    MakeServer: Fn(IceoryxStream) -> ServerTransport,
    MakeClient: Fn(IceoryxStream) -> ClientTransport,
{
    let runtime = tokio::runtime::Runtime::new().expect("tokio runtime");

    let base = unique_service_name();

    let (client, server) = runtime.block_on(async move {
        let server_stream =
            IceoryxStream::connect(&base, Role::Server, IceoryxConfig::default()).unwrap();
        let server_transport = make_server_transport(server_stream);
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
        let transport = make_client_transport(client_stream);
        let client = ArithmeticClient::new(Default::default(), transport).spawn();

        (client, server)
    });

    c.bench_function(name, |b| {
        b.to_async(&runtime).iter(|| async {
            let _ = client
                .add(context::current(), 1, 2)
                .await
                .expect("rpc call succeeds");
        });
    });

    runtime.block_on(async move {
        drop(client);
        server.abort();
    });
}

fn bench_roundtrip_frame<ServerTransport, ClientTransport, MakeServer, MakeClient>(
    c: &mut Criterion,
    name: &str,
    frame_len: usize,
    make_server_transport: MakeServer,
    make_client_transport: MakeClient,
) where
    ServerTransport: Transport<ServerSink, ServerItem> + Send + 'static,
    ClientTransport: Transport<ClientSink, ClientItem> + Send + 'static,
    MakeServer: Fn(IceoryxStream) -> ServerTransport,
    MakeClient: Fn(IceoryxStream) -> ClientTransport,
{
    let runtime = tokio::runtime::Runtime::new().expect("tokio runtime");

    let base = unique_service_name();

    let (client, server) = runtime.block_on(async move {
        let server_stream =
            IceoryxStream::connect(&base, Role::Server, IceoryxConfig::default()).unwrap();
        let server_transport = make_server_transport(server_stream);
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
        let transport = make_client_transport(client_stream);
        let client = ArithmeticClient::new(Default::default(), transport).spawn();

        (client, server)
    });

    let frame = vec![0u8; frame_len];

    c.bench_function(name, |b| {
        b.to_async(&runtime).iter(|| {
            let frame = frame.clone();
            async {
                let _ = client
                    .process_frame(context::current(), frame)
                    .await
                    .expect("rpc call succeeds");
            }
        });
    });

    runtime.block_on(async move {
        drop(client);
        server.abort();
    });
}

fn roundtrip_benchmark_bincode(c: &mut Criterion) {
    bench_roundtrip(
        c,
        "tarpc_roundtrip_bincode",
        bincode_transport,
        bincode_transport,
    );
}

fn roundtrip_benchmark_postcard(c: &mut Criterion) {
    bench_roundtrip(
        c,
        "tarpc_roundtrip_postcard",
        postcard_transport,
        postcard_transport,
    );
}

fn roundtrip_benchmark_bitcode(c: &mut Criterion) {
    bench_roundtrip(
        c,
        "tarpc_roundtrip_bitcode",
        bitcode_transport,
        bitcode_transport,
    );
}

fn frame_roundtrip_benchmark_bincode(c: &mut Criterion) {
    bench_roundtrip_frame(
        c,
        "tarpc_roundtrip_frame_bincode",
        1920 * 1080 * 3,
        bincode_transport,
        bincode_transport,
    );
}

fn frame_roundtrip_benchmark_postcard(c: &mut Criterion) {
    bench_roundtrip_frame(
        c,
        "tarpc_roundtrip_frame_postcard",
        1920 * 1080 * 3,
        postcard_transport,
        postcard_transport,
    );
}

fn frame_roundtrip_benchmark_bitcode(c: &mut Criterion) {
    bench_roundtrip_frame(
        c,
        "tarpc_roundtrip_frame_bitcode",
        1920 * 1080 * 3,
        bitcode_transport,
        bitcode_transport,
    );
}

fn rkyv_frame_serialization_benchmark(c: &mut Criterion) {
    let frame = vec![0u8; 1920 * 1080 * 3];

    c.bench_function("rkyv_frame_roundtrip", |b| {
        b.iter(|| {
            let encoded = to_bytes::<_, 256>(&frame).expect("rkyv encode succeeds");
            let decoded = from_bytes::<Vec<u8>>(&encoded).expect("rkyv decode succeeds");
            assert_eq!(decoded.len(), frame.len());
        });
    });
}

fn speedy_frame_serialization_benchmark(c: &mut Criterion) {
    let frame = vec![0u8; 1920 * 1080 * 3];

    c.bench_function("speedy_frame_roundtrip", |b| {
        b.iter(|| {
            let encoded = frame
                .write_to_vec_with_ctx(LittleEndian::default())
                .expect("speedy encode succeeds");
            let (decoded, consumed) =
                Vec::<u8>::read_with_length_from_buffer_with_ctx(LittleEndian::default(), &encoded);
            let decoded = decoded.expect("speedy decode succeeds");
            assert_eq!(consumed, encoded.len());
            assert_eq!(decoded.len(), frame.len());
        });
    });
}

criterion_group!(
    benches,
    roundtrip_benchmark_bincode,
    roundtrip_benchmark_postcard,
    roundtrip_benchmark_bitcode,
    frame_roundtrip_benchmark_bincode,
    frame_roundtrip_benchmark_postcard,
    frame_roundtrip_benchmark_bitcode,
    rkyv_frame_serialization_benchmark,
    speedy_frame_serialization_benchmark,
);
criterion_main!(benches);
