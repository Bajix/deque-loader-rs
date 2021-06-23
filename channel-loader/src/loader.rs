use crate::{
  request::Request,
  task::{Task, TaskHandler},
};
use crossbeam::{
  atomic::AtomicCell,
  deque::{Stealer, Worker},
};
use std::{
  sync::{Arc, RwLock, RwLockReadGuard},
  thread::LocalKey,
};
use tokio::sync::oneshot;

struct Stealers {}

/// Core load channel responsible for receiving incoming load_by requests to be enqueued via thread local [`crossbeam::deque::Worker`] queues
pub struct DataLoader<T: TaskHandler + 'static> {
  pub(crate) queue_size: &'static AtomicCell<usize>,
  pub(crate) queue: Worker<Request<T>>,
}

impl<T> DataLoader<T>
where
  T: TaskHandler,
{
  pub fn new(
    queue_size: &'static AtomicCell<usize>,
    task_stealers: &'static RwLock<Vec<Stealer<Request<T>>>>,
  ) -> Self {
    let queue = Worker::new_fifo();
    let mut task_stealers = task_stealers.write().unwrap();
    task_stealers.push(queue.stealer());

    Self { queue_size, queue }
  }

  pub fn load_by(&self, key: T::Key) -> oneshot::Receiver<Result<Option<Arc<T::Value>>, T::Error>>
  where
    T: TaskHandler,
  {
    let (req, rx) = Request::new(key);
    if self.queue_size.fetch_add(1).eq(&0) {
      tokio::task::spawn(async {
        T::handle_task(Task::new()).await;
      });
    }

    self.queue.push(req);

    rx
  }
}

pub trait StaticLoaderExt {
  fn loader() -> &'static LocalKey<DataLoader<Self>>
  where
    Self: TaskHandler;
  fn queue_size() -> &'static AtomicCell<usize>;
  fn task_stealers<'a>() -> RwLockReadGuard<'a, Vec<Stealer<Request<Self>>>>
  where
    Self: TaskHandler;
}
