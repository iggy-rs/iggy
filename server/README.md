# Iggy TPC (Thread-Per-Core)

This is a next version of the Iggy server using an alternative runtimes based on **thread-per-core model** and **io_uring** for asynchronous I/O based on **[monoio](https://github.com/bytedance/monoio)** runtime.

The goal is to compare the overall throughput and performance to the currently used approach based on the asynchronous **[tokio.rs](https://tokio.rs)** work-stealing runtime.
