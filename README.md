# Channel Loader

![License](https://img.shields.io/badge/license-MIT-green.svg)
[![Cargo](https://img.shields.io/crates/v/channel-loader.svg)](https://crates.io/crates/channel-loader)
[![Documentation](https://docs.rs/channel-loader/badge.svg)](https://docs.rs/channel-loader)

A work-stealing data loader designed around optimal load batching and connection utilization. As load requests return [`tokio::oneshot::Receiver`](https://docs.rs/tokio/1.6.1/tokio/sync/oneshot/struct.Receiver.html) and need not be immediately awaited, batching can be accomplished via type composition by embedding receivers in output types and delegating the responsibility to field resolvers to await the eventual values. This decouples query planning from query execution and provides greater flexibility of use, though optimal batching will occur irregardless of usage style. Should load requests occur after a load task handler has been spawned but prior to connection acquisition/task assignment, then this is batched so long as capacity permits. Load capacity is based off of the cardinality of the key set; duplicate requests for the same key clone results but otherwise don't count towards capacity. Altogether, this provides an elegant design pattern for deterministically batching loads without any timeout overhead and while maximizing connection utility under load.
