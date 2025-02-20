FROM rust:latest as builder
WORKDIR /build
COPY . /build
RUN cargo build --bin iggy --release
RUN cargo build --bin iggy-server --release

FROM gcr.io/distroless/cc
COPY configs ./configs
COPY --from=builder /build/target/release/iggy .
COPY --from=builder /build/target/release/iggy-server .
COPY --from=builder /usr/lib/x86_64-linux-gnu/liblzma.so.5 /usr/lib/x86_64-linux-gnu/

CMD ["/iggy-server"]
