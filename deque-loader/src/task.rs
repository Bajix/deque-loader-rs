use crate::{key::Key, request::Request};
use rayon::prelude::*;
use std::{
  collections::{HashMap, HashSet},
  marker::PhantomData,
  sync::Arc,
};
use swap_queue::Stealer;

/// A type-state control flow for driving tasks from assignment to completion. As task assignment can be deferred until connection acquisition and likewise loads batched by key, this enables opportunistic batching when connection acquisition becomes a bottleneck and also enables connection yielding as a consequence of work cancellation
#[async_trait::async_trait]
pub trait TaskHandler: Sized + Send + Sync + 'static {
  type Key: Key;
  type Value: Send + Sync + Clone + 'static;
  type Error: Send + Sync + Clone + 'static;
  const CORES_PER_WORKER_GROUP: usize = 4;
  const MAX_BATCH_SIZE: Option<usize> = None;
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
  pub(crate) stealer: Stealer<Request<K, V, E>>,
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
  pub(crate) fn new(stealer: Stealer<Request<K, V, E>>) -> Self {
    let requests = vec![];
    Task(PendingAssignment { stealer, requests })
  }

  // Work-steal all pending load tasks
  pub async fn get_assignment<T>(self) -> TaskAssignment<K, V, E>
  where
    T: TaskHandler,
    Self: Into<Task<PendingAssignment<T::Key, T::Value, T::Error>>>,
  {
    let PendingAssignment {
      stealer,
      mut requests,
    } = self.0;

    let batch = stealer.take().await;

    requests.extend(batch.into_iter());

    TaskAssignment::LoadBatch(Task::from_requests(requests))
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
    let keys: HashSet<K> =
      HashSet::from_par_iter(self.0.requests.par_iter().map(|req| req.key().to_owned()));
    keys.into_par_iter().collect()
  }

  #[must_use]
  pub fn resolve(self, results: Result<HashMap<K, Arc<V>>, E>) -> Task<CompletionReceipt> {
    let Task(LoadBatch { requests }) = self;

    rayon::spawn(move || {
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
    });

    Task::<CompletionReceipt>::completion_receipt()
  }

  #[must_use]
  pub(crate) fn apply_partial_results(
    self,
    results: HashMap<K, Arc<V>>,
  ) -> TaskAssignment<K, V, E> {
    let Task(LoadBatch { requests }) = self;

    let requests: Vec<Request<K, V, E>> = requests
      .into_par_iter()
      .filter_map(|req| {
        if let Some(value) = results.get(req.key()).cloned() {
          req.resolve(Ok(Some(value)));
          None
        } else {
          Some(req)
        }
      })
      .collect();

    if requests.len().gt(&0) {
      TaskAssignment::LoadBatch(Task::from_requests(requests))
    } else {
      TaskAssignment::NoAssignment(Task::completion_receipt())
    }
  }
}

impl Task<CompletionReceipt> {
  pub(crate) fn completion_receipt() -> Self {
    Task(CompletionReceipt(PhantomData))
  }
}
