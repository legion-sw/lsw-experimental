use tarpc::context;

/// Tarpc service that adds two 64-bit integers.
#[tarpc::service]
pub trait Adder {
    /// Adds two signed integers and returns the sum.
    async fn add(x: i64, y: i64) -> i64;
}

/// Server-side implementation of the [`Adder`] service.
#[derive(Clone, Default)]
pub struct AdderService;

impl Adder for AdderService {
    async fn add(self, _: context::Context, x: i64, y: i64) -> i64 {
        x + y
    }
}
