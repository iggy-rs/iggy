FROM rust:latest as builder
WORKDIR /build
COPY . /build
RUN cargo build --bin cli --release
RUN cargo build --bin server --release

FROM gcr.io/distroless/cc
COPY configs ./configs
COPY --from=builder /build/target/release/cli .
COPY --from=builder /build/target/release/server .

CMD ["/server"]