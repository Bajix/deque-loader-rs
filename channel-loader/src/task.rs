use crate::{key::Key, loader::StaticLoaderExt, request::Request};
use crossbeam::deque::Steal;
use itertools::Itertools;
use log::trace;
use rayon::prelude::*;
use std::{collections::HashMap, marker::PhantomData, sync::Arc};

/// A type-state control flow for driving tasks from assignment to completion. As task assignment can be deferred until connection acquisition and likewise loads batched by key, this enables opportunistic batching when connection acquisition becomes a bottleneck and also enables connection yielding as a consequence of work consolidation
#[async_trait::async_trait]
pub trait TaskHandler: StaticLoaderExt + Default + Send + Sync {
  type Key: Key;
  type Value: Send + Sync + Clone + 'static;
  type Error: Send + Sync + Clone + 'static;
  async fn handle_task(task: Task<PendingAssignment<Self>>) -> Task<CompletionReceipt<Self>>;
}

pub struct Task<T>(T);
/// A handle for deferred task assignment via work-stealing. Task assignement is deferred until connection acquisition to allow for opportunistic batching to occur
pub struct PendingAssignment<T: TaskHandler> {
  loader: PhantomData<fn() -> T>,
}

/// A batch of load requests, unique by key, to be loaded and the result resolved
pub struct LoadBatch<T: TaskHandler> {
  requests: Vec<Request<T>>,
}
/// An acknowledgement of task completion as to enforce a design contract that allows ownership of requests to be taken by the task handler.
/// This is a workaround to [rust-lang/rust#59337](https://github.com/rust-lang/rust/issues/59337) that enables task assignment to occur within a [`tokio::task::spawn_blocking`] closure
pub struct CompletionReceipt<T: TaskHandler> {
  loader: PhantomData<fn() -> T>,
}

/// A conditional assignment of work as a [`LoadBatch`]
pub enum TaskAssignment<T: TaskHandler> {
  /// A batch of keys to load values for
  LoadBatch(Task<LoadBatch<T>>),
  /// If other task handlers opportunistically resolve all tasks, there will be no task assignment and the handler can drop unused connections for use elsewhere
  NoAssignment(Task<CompletionReceipt<T>>),
}

impl<T> Task<PendingAssignment<T>>
where
  T: TaskHandler,
{
  #[must_use]
  pub(crate) fn new() -> Self {
    Task(PendingAssignment {
      loader: PhantomData,
    })
  }

  pub fn collect_tasks() -> Vec<Request<T>> {
    T::task_stealers()
      .clone()
      .into_iter()
      .flat_map(|stealer| {
        std::iter::from_fn(move || loop {
          match stealer.steal() {
            Steal::Success(req) => break Some(req),
            Steal::Retry => continue,
            Steal::Empty => break None,
          }
        })
      })
      .collect()
  }

  // Work-steal all pending load tasks
  pub fn get_assignment(self) -> TaskAssignment<T> {
    let mut requests = Vec::new();
    let mut queue_size = usize::MAX;

    while queue_size.gt(&0) {
      let batch = Task::<PendingAssignment<T>>::collect_tasks();
      let batch_len = batch.len();
      queue_size = T::queue_size().fetch_sub(batch_len) - batch_len;
      requests.extend(batch.into_iter());
    }

    if requests.len().gt(&0) {
      requests.par_sort_unstable_by(|a, b| a.key.cmp(&b.key));

      trace!(
        "{:?} assigned {} requests",
        core::any::type_name::<T>(),
        requests.len()
      );

      TaskAssignment::LoadBatch(Task::from_requests(requests))
    } else {
      trace!("{:?} assignment cancelled", core::any::type_name::<T>());

      TaskAssignment::NoAssignment(Task::resolve_receipt())
    }
  }
}

impl<T> Task<LoadBatch<T>>
where
  T: TaskHandler,
{
  fn from_requests(requests: Vec<Request<T>>) -> Self {
    Task(LoadBatch { requests })
  }

  pub fn keys(&self) -> Vec<T::Key> {
    self
      .0
      .requests
      .iter()
      .map(|req| &req.key)
      .dedup()
      .map(|k| k.to_owned())
      .collect_vec()
  }

  #[must_use]
  pub fn resolve(
    self,
    results: Result<HashMap<T::Key, Arc<T::Value>>, T::Error>,
  ) -> Task<CompletionReceipt<T>> {
    let Task(LoadBatch { requests }) = self;

    match results {
      Ok(values) => {
        requests
          .into_par_iter()
          .filter(|req| !req.tx.is_closed())
          .for_each(|req| {
            let value = values.get(&req.key).cloned();
            req.resolve(Ok(value));
          });
      }

      Err(e) => {
        requests
          .into_par_iter()
          .for_each(|req| req.resolve(Err(e.clone())));
      }
    };

    Task::<CompletionReceipt<T>>::resolve_receipt()
  }
}

impl<T> Task<CompletionReceipt<T>>
where
  T: TaskHandler,
{
  fn resolve_receipt() -> Self {
    Task(CompletionReceipt {
      loader: PhantomData,
    })
  }
}
