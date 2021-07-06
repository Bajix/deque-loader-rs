# Channel Loader

![License](https://img.shields.io/badge/license-MIT-green.svg)
[![Cargo](https://img.shields.io/crates/v/deque-loader.svg)](https://crates.io/crates/deque-loader)
[![Documentation](https://docs.rs/deque-loader/badge.svg)](https://docs.rs/deque-loader)

A work-stealing data loader designed around optimal load batching and connection utilization. Rather than using yields to collect batches of loads as would other data loaders, loads are enqueued to thread local dequeus and task handlers are spawned to meet capacity. As field resolvers within the same request will already be scheduled, subsequent loads within the same request will be scheduled prior to the load task handler taking a task assignment and upon connection acquisition, all load requests will have been captured and loads by separate requests can be opportunistically batched together without introducing timeout overhead.
