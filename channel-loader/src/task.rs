use crate::{key::Key, loader::StaticLoaderExt, request::Request};
use crossbeam::deque::{Steal, Stealer};
use itertools::Itertools;
use log::trace;
use std::{
  collections::HashMap,
  marker::PhantomData,
  sync::{Arc, RwLockReadGuard},
};

/// A type-state control flow for driving tasks from assignment to completion. As task assignment can be deferred until connection acquisition and likewise loads batched by key, this enables opportunistic batching when connection acquisition becomes a bottleneck and also enables connection yielding as a consequence of work consolidation
#[async_trait::async_trait]
pub trait TaskHandler: StaticLoaderExt + Default + Send + Sync {
  type Key: Key;
  type Value: Send + Clone + 'static;
  type Error: Send + Clone + 'static;
  async fn handle_task(task: Task<PendingAssignment<Self>>) -> Task<CompletionReceipt<Self>>;
}

pub struct Task<T>(T);
/// A handle for deferred task assignment via work-stealing. Task assignement is deferred until connection acquisition to allow for opportunistic batching to occur
pub struct PendingAssignment<T: TaskHandler> {
  stealer_index: usize,
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
  pub(crate) fn new(stealer_index: usize) -> Self {
    Task(PendingAssignment {
      stealer_index,
      loader: PhantomData,
    })
  }

  /// Steal a task by cycling through queues, starting at the index associated with the queue's task spawner. This maximizes colocality of loads enqueued together
  pub fn get_task<'a>(
    &self,
    stealers: &'a RwLockReadGuard<Vec<Stealer<Request<T>>>>,
  ) -> Option<Request<T>> {
    // We intentionally take all requests from a given stealer before taking any from the next
    for stealer in stealers
      .iter()
      .skip(self.0.stealer_index)
      .chain(stealers.iter().take(self.0.stealer_index))
    {
      loop {
        match stealer.steal() {
          Steal::Success(req) => return Some(req),
          Steal::Retry => continue,
          Steal::Empty => break,
        }
      }
    }
    None
  }

  // work-steal the largest possible task assignment from the corresponding [`crossbeam::deque::Worker`] of the associated [`DataLoader`]
  pub fn get_assignment(self) -> TaskAssignment<T> {
    let stealers = T::task_stealers();
    let mut requests: Vec<Request<T>> = Vec::new();
    loop {
      if let Some(req) = self.get_task(&stealers) {
        if !req.tx.is_closed() {
          requests.push(req);
        }

        if T::queue_size().fetch_sub(1).eq(&1) {
          break;
        }
      } else if T::queue_size().load().eq(&0) {
        break;
      }
    }

    if requests.len().gt(&0) {
      requests.sort_unstable_by(|a, b| a.key.cmp(&b.key));

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
      Ok(mut values) => {
        let mut iter = requests
          .into_iter()
          .filter(|req| !req.tx.is_closed())
          .peekable();

        while let Some(req) = iter.next() {
          let can_take_value = {
            if let Some(next_req) = iter.peek() {
              !next_req.key.eq(&req.key)
            } else {
              true
            }
          };

          let value = match can_take_value {
            true => values.remove(&req.key),
            false => values.get(&req.key).cloned(),
          };

          req.resolve(Ok(value));
        }
      }
      Err(e) => {
        for req in requests {
          req.resolve(Err(e.clone()));
        }
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
