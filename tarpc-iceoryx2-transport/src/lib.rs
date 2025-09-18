use std::{
    cmp::min,
    fmt,
    future::Future,
    io,
    pin::Pin,
    sync::atomic::{AtomicUsize, Ordering},
    task::{Context, Poll},
    time::Duration,
};

use iceoryx2::sample::Sample;
use iceoryx2::{
    node::{Node, NodeBuilder, node_name::NodeName},
    port::{
        publisher::Publisher,
        subscriber::{Subscriber, SubscriberCreateError},
    },
    service::{ipc_threadsafe::Service as IoxService, service_name::ServiceName},
};
use tarpc::tokio_serde::formats::Bincode;
use tokio::{
    io::{AsyncRead, AsyncWrite, ReadBuf},
    time::{Sleep, sleep},
};

/// Role of an endpoint connected via [`IceoryxStream`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Role {
    /// Endpoint that primarily listens for incoming RPC requests.
    Server,
    /// Endpoint that primarily initiates RPC requests.
    Client,
}

/// Configuration for an [`IceoryxStream`] instance.
#[derive(Clone, Debug)]
pub struct IceoryxConfig {
    /// Maximum payload size (in bytes) that can be transferred in a single message.
    pub max_message_size: usize,
    /// Number of messages that can be queued on the receiving side.
    pub subscriber_buffer_size: usize,
    /// Poll interval that is used when waiting for new data from iceoryx2.
    pub poll_interval: Duration,
}

impl Default for IceoryxConfig {
    fn default() -> Self {
        Self {
            max_message_size: 64 * 1024 * 1024,
            subscriber_buffer_size: 2,
            poll_interval: Duration::from_micros(50),
        }
    }
}

/// Bidirectional byte stream backed by iceoryx2 publish/subscribe services.
///
/// The stream is suitable as the IO primitive for [`tarpc::serde_transport`],
/// enabling tarpc RPC communication on top of iceoryx2.
pub struct IceoryxStream {
    publisher: Publisher<IoxService, [u8], ()>,
    subscriber: Subscriber<IoxService, [u8], ()>,
    current_sample: Option<Sample<IoxService, [u8], ()>>,
    sample_offset: usize,
    poll_interval: Duration,
    wait_state: WaitState,
    empty_poll_streak: u32,
    is_shutdown: bool,
    _node: Node<IoxService>,
}

impl fmt::Debug for IceoryxStream {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("IceoryxStream")
            .field(
                "pending_sample_len",
                &self
                    .current_sample
                    .as_ref()
                    .map(|sample| sample.len().saturating_sub(self.sample_offset)),
            )
            .field("is_shutdown", &self.is_shutdown)
            .finish()
    }
}

impl IceoryxStream {
    /// Establishes a new [`IceoryxStream`] using the provided base service name and role.
    ///
    /// Both the client and server must invoke this constructor with the same base
    /// service name but different [`Role`]s to communicate with each other.
    pub fn connect(base_service: &str, role: Role, config: IceoryxConfig) -> io::Result<Self> {
        if base_service.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "base service name must not be empty",
            ));
        }

        let node = create_node(base_service, role)?;
        let (send_name, recv_name) = direction_service_names(base_service, role)?;

        let publisher = create_publisher(&node, &send_name, config.max_message_size)?;
        let subscriber = create_subscriber(&node, &recv_name, config.subscriber_buffer_size)?;

        Ok(Self {
            publisher,
            subscriber,
            current_sample: None,
            sample_offset: 0,
            poll_interval: config.poll_interval,
            wait_state: WaitState::None,
            empty_poll_streak: 0,
            is_shutdown: false,
            _node: node,
        })
    }

    fn send_bytes(&self, buf: &[u8]) -> io::Result<()> {
        if buf.is_empty() {
            return Ok(());
        }

        let mut sample = self.publisher.loan_slice(buf.len()).map_err(to_io_error)?;
        sample.copy_from_slice(buf);
        sample.send().map_err(to_io_error)?;
        Ok(())
    }
}

impl AsyncRead for IceoryxStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if buf.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }

        let this = self.as_mut().get_mut();

        loop {
            match &mut this.wait_state {
                WaitState::None => {}
                WaitState::Yield(fut) => match Pin::new(fut).poll(cx) {
                    Poll::Ready(()) => {
                        this.wait_state = WaitState::None;
                        continue;
                    }
                    Poll::Pending => return Poll::Pending,
                },
                WaitState::Sleep(fut) => match fut.as_mut().poll(cx) {
                    Poll::Ready(_) => {
                        this.wait_state = WaitState::None;
                        continue;
                    }
                    Poll::Pending => return Poll::Pending,
                },
            }

            if let Some(sample) = this.current_sample.take() {
                let sample_len = sample.len();

                if this.sample_offset >= sample_len {
                    this.sample_offset = 0;
                    continue;
                }

                let available = &sample[this.sample_offset..];
                let to_copy = min(buf.remaining(), available.len());
                buf.put_slice(&available[..to_copy]);
                this.sample_offset += to_copy;

                if this.sample_offset < sample_len {
                    this.current_sample = Some(sample);
                } else {
                    this.sample_offset = 0;
                }

                return Poll::Ready(Ok(()));
            }

            if this.is_shutdown {
                return Poll::Ready(Ok(()));
            }

            match this.subscriber.receive() {
                Ok(Some(sample)) => {
                    this.current_sample = Some(sample);
                    this.sample_offset = 0;
                    this.wait_state = WaitState::None;
                    this.empty_poll_streak = 0;
                    continue;
                }
                Ok(None) => {
                    this.empty_poll_streak = this
                        .empty_poll_streak
                        .saturating_add(1)
                        .min(FAST_POLL_SPINS);
                    let should_sleep = this.poll_interval != Duration::ZERO
                        && this.empty_poll_streak >= FAST_POLL_SPINS;

                    this.wait_state = if should_sleep {
                        WaitState::Sleep(Box::pin(sleep(this.poll_interval)))
                    } else {
                        WaitState::Yield(YieldNowFuture::new())
                    };
                    continue;
                }
                Err(err) => {
                    let io_err = to_io_error(err);
                    return Poll::Ready(Err(io_err));
                }
            }
        }
    }
}

impl AsyncWrite for IceoryxStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.as_mut().get_mut();

        if this.is_shutdown {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "stream is shut down",
            )));
        }

        match this.send_bytes(buf) {
            Ok(()) => Poll::Ready(Ok(buf.len())),
            Err(err) => Poll::Ready(Err(err)),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        this.is_shutdown = true;
        this.wait_state = WaitState::None;
        Poll::Ready(Ok(()))
    }
}

const FAST_POLL_SPINS: u32 = 4096;

enum WaitState {
    None,
    Yield(YieldNowFuture),
    Sleep(Pin<Box<Sleep>>),
}

struct YieldNowFuture {
    yielded: bool,
}

impl YieldNowFuture {
    fn new() -> Self {
        Self { yielded: false }
    }
}

impl Future for YieldNowFuture {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if self.yielded {
            Poll::Ready(())
        } else {
            self.yielded = true;
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    }
}

impl Unpin for YieldNowFuture {}

/// Creates a tarpc transport that serializes messages with [`Bincode`].
pub fn bincode_transport<Item, SinkItem>(
    stream: IceoryxStream,
) -> tarpc::serde_transport::Transport<IceoryxStream, Item, SinkItem, Bincode<Item, SinkItem>>
where
    Item: for<'de> serde::Deserialize<'de>,
    SinkItem: serde::Serialize,
{
    tarpc::serde_transport::Transport::from((stream, Bincode::default()))
}

fn create_publisher(
    node: &Node<IoxService>,
    service: &ServiceName,
    max_message_size: usize,
) -> io::Result<Publisher<IoxService, [u8], ()>> {
    let port_factory = node
        .service_builder(service)
        .publish_subscribe::<[u8]>()
        .open_or_create()
        .map_err(to_io_error)?;
    port_factory
        .publisher_builder()
        .initial_max_slice_len(max_message_size.max(1))
        .create()
        .map_err(to_io_error)
}

fn create_subscriber(
    node: &Node<IoxService>,
    service: &ServiceName,
    buffer_size: usize,
) -> io::Result<Subscriber<IoxService, [u8], ()>> {
    let port_factory = node
        .service_builder(service)
        .publish_subscribe::<[u8]>()
        .open_or_create()
        .map_err(to_io_error)?;
    port_factory
        .subscriber_builder()
        .buffer_size(buffer_size.max(1))
        .create()
        .map_err(|err| match err {
            SubscriberCreateError::BufferSizeExceedsMaxSupportedBufferSizeOfService => {
                io::Error::new(io::ErrorKind::InvalidInput, err.to_string())
            }
            other => to_io_error(other),
        })
}

fn direction_service_names(base: &str, role: Role) -> io::Result<(ServiceName, ServiceName)> {
    let (out_suffix, in_suffix) = match role {
        Role::Server => ("server_to_client", "client_to_server"),
        Role::Client => ("client_to_server", "server_to_client"),
    };

    let send = ServiceName::new(&format!("{base}/{out_suffix}")).map_err(to_io_error)?;
    let recv = ServiceName::new(&format!("{base}/{in_suffix}")).map_err(to_io_error)?;
    Ok((send, recv))
}

fn create_node(base: &str, role: Role) -> io::Result<Node<IoxService>> {
    static COUNTER: AtomicUsize = AtomicUsize::new(0);
    let id = COUNTER.fetch_add(1, Ordering::Relaxed);
    let sanitized_base = base.replace('/', "_");
    let node_name = NodeName::new(&format!(
        "tarpc_{}_{}_{}_{}",
        std::process::id(),
        match role {
            Role::Server => "server",
            Role::Client => "client",
        },
        sanitized_base,
        id,
    ))
    .map_err(to_io_error)?;

    NodeBuilder::new()
        .name(&node_name)
        .create::<IoxService>()
        .map_err(to_io_error)
}

fn to_io_error<E: fmt::Display>(err: E) -> io::Error {
    io::Error::other(err.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;
    use tarpc::{
        context,
        server::{BaseChannel, Channel},
    };
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

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
        format!("tarpc/test/{}/{}", std::process::id(), id)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn tarpc_roundtrip() -> io::Result<()> {
        let base = unique_service_name();

        let server_stream = IceoryxStream::connect(&base, Role::Server, IceoryxConfig::default())?;
        let server_transport = bincode_transport(server_stream);
        let server = tokio::spawn(async move {
            BaseChannel::with_defaults(server_transport)
                .execute(ArithmeticImpl.serve())
                .for_each(|fut| async move {
                    tokio::spawn(fut);
                })
                .await;
        });

        let client_stream = IceoryxStream::connect(&base, Role::Client, IceoryxConfig::default())?;
        let transport = bincode_transport(client_stream);
        let client = ArithmeticClient::new(Default::default(), transport).spawn();

        let result = client
            .add(context::current(), 3, 4)
            .await
            .expect("rpc call should succeed");
        assert_eq!(result, 7);

        drop(client);
        server.abort();
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn transfers_large_payload_in_chunks() -> io::Result<()> {
        let base = unique_service_name();

        let mut server_stream =
            IceoryxStream::connect(&base, Role::Server, IceoryxConfig::default())?;
        let mut client_stream =
            IceoryxStream::connect(&base, Role::Client, IceoryxConfig::default())?;

        let payload = vec![0xAB; 1_048_576];
        let sender_payload = payload.clone();
        let sender = tokio::spawn(async move {
            server_stream.write_all(&sender_payload).await?;
            server_stream.shutdown().await?;
            Ok::<_, io::Error>(())
        });

        let mut received = Vec::with_capacity(payload.len());
        let mut buffer = vec![0u8; 32 * 1024];
        while received.len() < payload.len() {
            let read = client_stream.read(&mut buffer).await?;
            assert!(read > 0, "stream ended before full payload was received");
            received.extend_from_slice(&buffer[..read]);
        }

        sender.await.expect("sender task should complete")?;
        assert_eq!(received, payload);
        Ok(())
    }
}
