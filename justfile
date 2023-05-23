test:
    cargo test

build:
    cargo build

run:
    cargo run

coverage:
    cargo tarpaulin --out html
    open tarpaulin-report.html