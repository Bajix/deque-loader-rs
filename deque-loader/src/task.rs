use crate::{key::Key, request::Request, worker::QueueHandle};
use itertools::Itertools;
use rayon::prelude::*;
use std::{collections::HashMap, marker::PhantomData, sync::Arc};

/// A type-state control flow for driving tasks from assignment to completion. As task assignment can be deferred until connection acquisition and likewise loads batched by key, this enables opportunistic batching when connection acquisition becomes a bottleneck and also enables connection yielding as a consequence of work cancellation
#[async_trait::async_trait]
pub trait TaskHandler: Sized + Send + Sync + 'static {
  type Key: Key;
  type Value: Send + Sync + Clone + 'static;
  type Error: Send + Sync + Clone + 'static;
  const CORES_PER_WORKER_GROUP: usize = 4;
  async fn handle_task(
    task: Task<PendingAssignment<Self::Key, Self::Value, Self::Error>>,
  ) -> Task<CompletionReceipt>;
}
pub struct Task<T>(pub(crate) T);
/// A handle for deferred task assignment via work-stealing. Task assignement is deferred until connection acquisition to allow for opportunistic batching to occur
pub struct PendingAssignment<
  K: Key,
  V: Send + Sync + Clone + 'static,
  E: Send + Sync + Clone + 'static,
> {
  pub(crate) queue_handle: &'static QueueHandle<K, V, E>,
  pub(crate) requests: Vec<Request<K, V, E>>,
}

/// A batch of load requests, unique by key, to be loaded and the result resolved
pub struct LoadBatch<K: Key, V: Send + Sync + Clone + 'static, E: Send + Sync + Clone + 'static> {
  pub(crate) requests: Vec<Request<K, V, E>>,
}
/// An acknowledgement of task completion as to enforce a design contract that allows ownership of requests to be taken by the task handler.
/// This is a workaround to [rust-lang/rust#59337](https://github.com/rust-lang/rust/issues/59337) that enables task assignment to occur within a [`tokio::task::spawn_blocking`] closure
pub struct CompletionReceipt(PhantomData<fn() -> ()>);

/// A conditional assignment of work as a [`LoadBatch`]
pub enum TaskAssignment<K: Key, V: Send + Sync + Clone + 'static, E: Send + Sync + Clone + 'static>
{
  /// A batch of keys to load values for
  LoadBatch(Task<LoadBatch<K, V, E>>),
  /// If other task handlers opportunistically resolve all tasks, there will be no task assignment and the handler can drop unused connections for use elsewhere
  NoAssignment(Task<CompletionReceipt>),
}

impl<K, V, E> Task<PendingAssignment<K, V, E>>
where
  K: Key,
  V: Send + Sync + Clone + 'static,
  E: Send + Sync + Clone + 'static,
{
  #[must_use]
  pub(crate) fn new(queue_handle: &'static QueueHandle<K, V, E>) -> Self {
    let requests = vec![];
    Task(PendingAssignment {
      queue_handle,
      requests,
    })
  }

  // Work-steal all pending load tasks
  pub fn get_assignment(self) -> TaskAssignment<K, V, E> {
    let mut requests = self.0.queue_handle.drain_queue();

    requests.extend(self.0.requests);

    if requests.len().gt(&0) {
      requests.par_sort_unstable_by(|a, b| a.key().cmp(b.key()));

      TaskAssignment::LoadBatch(Task::from_requests(requests))
    } else {
      TaskAssignment::NoAssignment(Task::resolve_receipt())
    }
  }
}

impl<K, V, E> Task<LoadBatch<K, V, E>>
where
  K: Key,
  V: Send + Sync + Clone + 'static,
  E: Send + Sync + Clone + 'static,
{
  pub(crate) fn from_requests(requests: Vec<Request<K, V, E>>) -> Self {
    Task(LoadBatch { requests })
  }

  pub fn keys(&self) -> Vec<K> {
    self
      .0
      .requests
      .iter()
      .map(|req| req.key())
      .dedup()
      .map(|k| k.to_owned())
      .collect_vec()
  }

  #[must_use]
  pub fn resolve(self, results: Result<HashMap<K, Arc<V>>, E>) -> Task<CompletionReceipt> {
    let Task(LoadBatch { requests }) = self;

    match results {
      Ok(values) => {
        requests.into_par_iter().for_each(|req| {
          let value = values.get(req.key()).cloned();
          req.resolve(Ok(value));
        });
      }

      Err(e) => {
        requests
          .into_par_iter()
          .for_each(|req| req.resolve(Err(e.clone())));
      }
    };

    Task::<CompletionReceipt>::resolve_receipt()
  }
}

impl Task<CompletionReceipt> {
  pub(crate) fn resolve_receipt() -> Self {
    Task(CompletionReceipt(PhantomData))
  }
}
