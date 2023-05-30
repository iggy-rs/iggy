![iggy](assets/iggy.png)

# Iggy

---

Iggy is the persistent message streaming platform written in Rust, using [QUIC](https://www.chromium.org/quic/) (custom binary specification) and/or HTTP (regular REST API) as a transport protocol . Currently, running as a single server, it allows creating streams, topics, partitions and segments, and send/receive messages to/from them. The messages are stored on disk as an append-only log, and are persisted between restarts.

The goal of the project is to make a distributed streaming platform (running as a cluster), which will be able to handle millions of messages per second and scale horizontally.

It is a pet project of mine to learn more about distributed systems and Rust. The name is an abbreviation for the Italian Greyhound - small yet extremely fast dogs, the best in their class. Just like mine lovely [Fabio & Cookie](https://www.instagram.com/fabio.and.cookie/) ❤️

---

### Basic usage

Build the project (the longer compilation time is due to [LTO](https://doc.rust-lang.org/rustc/linker-plugin-lto.html) enabled in release [profile](https://github.com/spetz/iggy/blob/master/Cargo.toml#L2)):

`cargo build -r`

Run the tests:

`cargo test`

Start the server:

`cargo r --bin server -r`

Start the client:

`cargo r --bin client -r`

Create a stream named `dev` with ID 1:

`stream.create|1|dev`

List available streams:

`stream.list`

Create a topic named `dummy` with ID 1 and 2 partitions (IDs 1 and 2) for stream `dev` (ID 1):

`topic.create|1|1|2|dummy`

List available topics for stream `dev` (ID 1):

`topic.list|1`

Send a message to the stream `dev` (ID 1) to topic `dummy` (ID 1) and partition 1:

`message.send|1|1|p|1|hello world`

Send another message:

`message.send|1|1|p|1|lorem ipsum`

Poll 2 messages by an arbitrary consumer 0 from the stream `dev` (ID 1) for topic `dummy` (ID 1) and partition with ID 1, starting with offset (`o`) 0, without auto commit (`n`) (storing consumer offset on server) and using string format `s` to render messages payload:

`message.poll|0|1|1|1|o|0|2|n|s`

Finally, restart the server to see it is able to load the persisted data.

To see the detailed logs from the client/server, run it with `RUST_LOG=trace` environment variable.

To benchmark the project, first start the server and then run the benchmarking app:

`cargo r --bin bench -r -- --http --messages-per-batch 1000 --message-batches 1000 --message-size 1000 --clients-count 10 --test-send-messages`

`cargo r --bin bench -r -- --http --messages-per-batch 1000 --message-batches 1000 --message-size 1000 --clients-count 10 --test-poll-messages`

Depending on the hardware, settings in `server.json`, transport protocol (`quic` or `http`) and payload size (`messages-per-batch * message-size`) you might expect over 2000 MB/s (e.g. 2M of 1 KB messages per second) throughput for both writes and reads. The current results have been achieved on Apple M1 Max with 64 GB RAM.

**See the images below**

Files structure

![files structure](assets/files_structure.png)

Server start

![server start](assets/server_start.png)

Client start

![client start](assets/client_start.png)

Test mode

![client start](assets/test_mode.png)

Server restart

![server restart](assets/server_restart.png)

---

### Features and TODO

#### Project

- [x] Setup workspace for different projects
- [x] Create granular components with their own behavior and logic
- [x] Define custom conventions such as error types, statuses etc.
- [x] Make use of logging and observability crates
- [x] Create the benchmarking app to test client/server performance
- [x] Implement unit tests
- [x] Implement integration tests
- [x] Implement end-to-end tests
- [ ] Make use of `async trait` once available in stable Rust

#### Server

- [x] Create a basic UDP server
- [x] Make use of QUIC protocol
- [ ] Extend QUIC configuration with custom certificates
- [x] Use async runtime from tokio
- [x] Define the custom binary protocol for communication
- [x] Allow multiple clients to connect to the server
- [x] Provide configuration via terminal arguments
- [x] Provide configuration via custom configuration file
- [x] Implement the graceful shutdown

#### Client

- [x] Create a basic UDP client
- [x] Make use of QUIC protocol
- [ ] Extend QUIC configuration with custom certificates
- [x] Provide configuration via terminal arguments
- [ ] Provide configuration via custom configuration file
- [x] Communicate with the server using established binary protocol
- [x] Allow to send commands to the server via simple CLI
- [x] Parse input from the CLI & handle the received response
- [ ] Keep the history of the commands in the CLI
- [ ] Create a simple terminal UI for the client

#### SDK

- [X] Implement the QUIC SDK for the client
- [ ] Implement the HTTP SDK for the client
- [X] Make use of the SDK in client project
- [ ] Implement another SDK in C# for dotnet clients

#### Streaming

- [x] Implement basic structures such as `stream`, `topic`, `partition`, `segment` etc.
- [x] Encapsulate the logic of reading and writing to the stream
- [x] Persist the stream structure to the disk & load it on startup
- [x] Implement `Streams` consisting of multiple `Topics`
- [x] Implement `Topic` consisting of multiple `Partitions`
- [x] Implement `Partition` consisting of multiple `Segments`
- [x] Store `Stream → Topic → Partition → Segment` structures on the disk in the separate directories
- [x] Store messages on disk as append-only log using binary format
- [x] Store messages indexes and time indexes on disk for fast access
- [x] Automatically create new partition segments when the current one is full
- [x] Allow clients to create/read/delete topics
- [x] Allow clients to send messages to the specific stream, topic and partition
- [x] Allow clients to poll messages by offsets from the specific partition
- [x] Allow clients to poll messages by timestamps from the specific partition
- [x] Make use of ring buffer to cache in-memory the latest messages to allow fast access to them
- [X] Index messages by their offset to allow fast access to the specific messages
- [X] Index messages by their timestamp to allow fast access to the specific messages
- [X] Allow parallel reading/writing from/to the distinct partitions
- [X] Allow storing client offset for the specific partition
- [ ] Implement efficient message writing on disk
- [ ] Implement efficient message reading from disk
- [ ] Implement zero-copy message reading from disk → sending to network buffer
- [ ] Implement message deduplication
- [ ] Implement consumer groups for horizontal scaling
- [ ] Delete old messages bases on retention policy

#### Distribution

- [ ] Implement consensus protocol for the cluster
- [ ] Implement leader election for the cluster
- [ ] Implement cluster membership protocol
- [ ] Implement cluster discovery protocol
- [ ] Implement cluster configuration protocol
- [ ] Implement cluster state replication protocol
- [ ] Implement cluster state synchronization protocol
- [ ] Implement partition replication protocol on different servers
- [ ] Allow clients to connect to the cluster

#### API

- [x] Implement REST API for the server using Axum
- [x] Expose all the routes to achieve the same functionality as with the QUIC
- [ ] Generate OpenAPI specification for the REST API

#### UI

- [ ] Build a simple UI for the server using chosen framework