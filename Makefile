debug:
	cargo build --all
	RUST_LOG=libaster=debug RUST_BACKTRACE=full ./target/debug/rcproxy default.toml

release:
	cargo build --all --release
	RUST_LOG=libaster=info RUST_BACKTRACE=full ./target/release/rcproxy default.toml

test:
	cargo test --all

bench:
	cargo bench

clean:
	cargo clean

metrics:
	cargo build --manifest-path ./libaster/Cargo.toml --features metrics