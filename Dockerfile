FROM rust:latest as builder
WORKDIR /build
COPY . /build
RUN cargo build --bin client --release
RUN cargo build --bin server --release

FROM gcr.io/distroless/cc
COPY configs ./configs
COPY --from=builder /build/target/release/client .
COPY --from=builder /build/target/release/server .

CMD ["/server"]