use crate::task::TaskHandler;
use flurry::HashMap;
use std::sync::Arc;
use tokio::sync::{oneshot, watch};

pub enum LoadState<T: TaskHandler> {
  Ready(Result<Option<Arc<T::Value>>, T::Error>),
  Pending,
}

#[derive(Clone)]
pub struct WatchReceiver<T: TaskHandler>(watch::Receiver<LoadState<T>>);

pub struct OneshotReceiver<T: TaskHandler>(
  oneshot::Receiver<Result<Option<Arc<T::Value>>, T::Error>>,
);

impl<T> From<watch::Receiver<LoadState<T>>> for WatchReceiver<T>
where
  T: TaskHandler,
{
  fn from(rx: watch::Receiver<LoadState<T>>) -> Self {
    WatchReceiver(rx)
  }
}

impl<T> From<oneshot::Receiver<Result<Option<Arc<T::Value>>, T::Error>>> for OneshotReceiver<T>
where
  T: TaskHandler,
{
  fn from(rx: oneshot::Receiver<Result<Option<Arc<T::Value>>, T::Error>>) -> Self {
    OneshotReceiver(rx)
  }
}

impl<T> WatchReceiver<T>
where
  T: TaskHandler,
{
  pub async fn recv(mut self) -> Result<Option<Arc<T::Value>>, T::Error> {
    loop {
      if let LoadState::Ready(ref result) = *self.0.borrow() {
        break result.to_owned();
      }

      self.0.changed().await.unwrap();
    }
  }
}

impl<T> OneshotReceiver<T>
where
  T: TaskHandler,
{
  pub async fn recv(self) -> Result<Option<Arc<T::Value>>, T::Error> {
    self.0.await.unwrap()
  }
}

pub enum Request<T: TaskHandler> {
  Watch {
    key: T::Key,
    tx: watch::Sender<LoadState<T>>,
  },
  Oneshot {
    key: T::Key,
    tx: oneshot::Sender<Result<Option<Arc<T::Value>>, T::Error>>,
  },
}

impl<T: TaskHandler> Request<T> {
  pub(crate) fn new_oneshot(key: T::Key) -> (Request<T>, OneshotReceiver<T>) {
    let (tx, rx) = oneshot::channel();

    let request = Request::Oneshot { key, tx };

    (request, rx.into())
  }

  pub(crate) fn new_watch(key: T::Key) -> (Request<T>, WatchReceiver<T>) {
    let (tx, rx) = watch::channel(LoadState::Pending);

    let request = Request::Watch { key, tx };

    (request, rx.into())
  }

  pub(crate) fn key(&self) -> &T::Key {
    match self {
      Request::Watch { key, .. } => key,
      Request::Oneshot { key, .. } => key,
    }
  }

  pub(crate) fn resolve(self, value: Result<Option<Arc<T::Value>>, T::Error>) {
    match self {
      Request::Watch { tx, .. } => {
        if !tx.is_closed() {
          tx.send(LoadState::Ready(value)).ok();
        }
      }
      Request::Oneshot { tx, .. } => {
        if !tx.is_closed() {
          tx.send(value).ok();
        }
      }
    };
  }
}
pub struct LoadCache<T: TaskHandler> {
  data: HashMap<T::Key, watch::Receiver<LoadState<T>>>,
}

impl<T> LoadCache<T>
where
  T: TaskHandler,
{
  pub fn new() -> Self {
    LoadCache {
      data: HashMap::new(),
    }
  }

  pub(crate) fn get_or_create(&self, key: &T::Key) -> (WatchReceiver<T>, Option<Request<T>>) {
    let guard = self.data.guard();

    loop {
      if let Some(rx) = self.data.get(key, &guard) {
        break (rx.clone().into(), None);
      }

      let (req, rx) = Request::<T>::new_watch(key.to_owned());

      match self.data.try_insert(key.clone(), rx.0, &guard) {
        Ok(rx) => break (rx.to_owned().into(), Some(req)),
        Err(_) => continue,
      }
    }
  }
}
