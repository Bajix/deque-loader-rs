use std::sync::Arc;

use crate::task::TaskHandler;
use tokio::sync::{oneshot, watch};

pub enum LoadState<T: TaskHandler> {
  Ready(Result<Option<Arc<T::Value>>, T::Error>),
  Pending,
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
  pub(crate) fn new_oneshot(
    key: T::Key,
  ) -> (
    Request<T>,
    oneshot::Receiver<Result<Option<Arc<T::Value>>, T::Error>>,
  ) {
    let (tx, rx) = oneshot::channel();

    let request = Request::Oneshot { key, tx };

    (request, rx)
  }

  pub(crate) fn new_watch(key: T::Key) -> (Request<T>, watch::Receiver<LoadState<T>>) {
    let (tx, rx) = watch::channel(LoadState::Pending);

    let request = Request::Watch { key, tx };

    (request, rx)
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
