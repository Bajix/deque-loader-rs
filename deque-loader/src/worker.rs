use crate::{loader::DataLoader, request::Request, task::TaskHandler, Key};
use atomic_take::AtomicTake;
use crossbeam::{
  atomic::AtomicCell,
  deque::{Steal, Stealer, Worker},
};
use itertools::Itertools;
use num::Integer;
use rayon::prelude::*;
use std::iter;
pub struct QueueHandle<K: Key, V: Send + Sync + Clone + 'static, E: Send + Sync + Clone + 'static> {
  pub(crate) queue_size: AtomicCell<usize>,
  stealers: Vec<Stealer<Request<K, V, E>>>,
}

impl<K, V, E> QueueHandle<K, V, E>
where
  K: Key,
  V: Send + Sync + Clone + 'static,
  E: Send + Sync + Clone + 'static,
{
  fn new(stealers: Vec<Stealer<Request<K, V, E>>>) -> Self {
    QueueHandle {
      queue_size: AtomicCell::new(0),
      stealers,
    }
  }

  fn collect_tasks(&self) -> Vec<Request<K, V, E>> {
    self
      .stealers
      .par_iter()
      .flat_map_iter(|stealer| {
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

  pub(crate) fn collect_queue(&self, requests: &mut Vec<Request<K, V, E>>) {
    loop {
      let batch = self.collect_tasks();
      let batch_len = batch.len();

      requests.extend(batch.into_iter());

      if self.queue_size.fetch_sub(batch_len).le(&batch_len) {
        break;
      }
    }
  }
}

pub struct WorkerGroup<T: TaskHandler> {
  queue_handle: QueueHandle<T::Key, T::Value, T::Error>,
  workers: Vec<AtomicTake<Worker<Request<T::Key, T::Value, T::Error>>>>,
}

impl<T> WorkerGroup<T>
where
  T: TaskHandler,
{
  fn new(set_size: usize) -> Self {
    let workers = iter::repeat_with(Worker::new_fifo)
      .take(set_size)
      .collect_vec();

    let stealers = workers.iter().map(|worker| worker.stealer()).collect_vec();

    let queue_handle = QueueHandle::new(stealers);

    let workers = workers.into_iter().map(AtomicTake::new).collect_vec();

    WorkerGroup::<T> {
      queue_handle,
      workers,
    }
  }
}

/// Static worker group registry as to batch loads across groups of thread local [`DataLoader`] instances
pub struct WorkerRegistry<T: TaskHandler> {
  claim_counter: AtomicCell<usize>,
  worker_groups: Vec<WorkerGroup<T>>,
}

impl<T> WorkerRegistry<T>
where
  T: TaskHandler,
{
  /// Pre-allocate workers for each potential thread local [`DataLoader`]
  pub fn new() -> Self {
    let core_count = num_cpus::get();
    let group_size = T::CORES_PER_WORKER_GROUP.min(Integer::div_ceil(&core_count, &2));
    let group_count = Integer::div_ceil(&core_count, &group_size);

    let claim_counter = AtomicCell::new(0);

    let worker_groups = iter::repeat_with(|| WorkerGroup::<T>::new(group_size))
      .take(group_count)
      .collect_vec();

    WorkerRegistry {
      claim_counter,
      worker_groups,
    }
  }

  /// Take pre-allocated workers by cycling through groups
  pub(crate) fn take_loader(&'static self) -> Option<DataLoader<T>> {
    let WorkerRegistry {
      claim_counter,
      worker_groups,
    } = self;

    let group_count = self.worker_groups.len();

    let slot = claim_counter.fetch_add(1);

    let group_index = slot % group_count;
    let worker_index = Integer::div_floor(&slot, &group_count);

    let worker_group = worker_groups.get(group_index)?;
    let queue = worker_group.workers.get(worker_index)?.take()?;

    let queue_handle = &worker_group.queue_handle;

    Some(DataLoader::new(queue, queue_handle))
  }
}
