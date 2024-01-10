FROM rust:latest as builder
WORKDIR /build
COPY . /build
RUN cargo build --bin iggy --release
RUN cargo build --bin iggy-server --release

FROM gcr.io/distroless/cc
COPY configs ./configs
COPY --from=builder /build/target/release/iggy .
COPY --from=builder /build/target/release/iggy-server .

CMD ["/iggy-server"]
