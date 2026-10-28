# Contributing

Bug reports and pull requests are welcome. For anything bigger than a small
fix, open an issue first so we can agree on the approach before you spend
time on it. Security problems go to the address in [SECURITY.md](SECURITY.md),
not the issue tracker.

## Reporting a bug

Say which walship and Rust versions you're on, the operating system and
filesystem of both machines, and what the shipper's `status()` and the
receiver's `GET /replica/{name}` showed. If a replica ended up different from
its primary, the smallest program that gets it there is worth more than any
log.

## Building and testing

You need Rust 1.88 or later and a C compiler, since `libsql-ffi` builds
SQLite from source. CI runs these, so run them before you push:

```sh
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-features
cargo doc --no-deps --all-features
```

Each feature also has to build on its own:

```sh
cargo check --no-default-features
cargo check --no-default-features --features shipper
cargo check --no-default-features --features server
```

`examples/stress.rs` runs a primary and a replica side by side under load and
checks that they end up with the same rows. Run it when you touch the WAL code
or the receiver. The arguments are a directory, seconds to run and a simulated
round trip in milliseconds:

```sh
cargo run --release --example stress -- /tmp/walship-stress 10 20
```

## Changes to the code

- Keep the minimum Rust version at 1.88. Raising it is a decision of its own,
  in its own pull request.
- New behaviour comes with a test. A fix comes with a test that fails without
  it.
- libSQL's WAL functions have undocumented behaviour that walship relies on
  (see "libSQL notes" in the README). If you find more of it, pin it down with
  a test in `src/wal.rs`.
- Every `unsafe` block gets a `SAFETY:` comment saying why it holds.
- Add a line to `CHANGELOG.md` for anything a user would notice.

## The libsql patch

`patches/libsql-0.9.30.patch` is checked by the `libsql-patch` job in CI,
which follows the README's steps against the published crate. If you change
the patch or those steps, change both in the same pull request.

## License

Unless you explicitly state otherwise, any contribution intentionally
submitted for inclusion in walship by you, as defined in the Apache-2.0
license, shall be dual licensed as in the README, without any additional
terms or conditions.
