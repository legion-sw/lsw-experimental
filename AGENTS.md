# Instructions for Contributors

This repository contains a Rust CLI application. When making code changes:

- Format Rust code with `cargo fmt --all`.
- Run the full test suite with `cargo test --locked --quiet`.
- Builds and tests must be warning free; use `RUSTFLAGS="-D warnings"`.
- Run `cargo clippy --all-targets -- -D warnings` and fix any lints.
- Generate code coverage using `cargo llvm-cov --locked --quiet`. Use the
  resulting coverage information to judge the effectiveness of your tests and
  improve them when coverage is lacking.
- Use 4-space indentation in Rust source files.
- Commit messages should be descriptive.
- Always add tests for new functionality.
- Review coverage results for code you change and ensure new code is
  adequately exercised.

