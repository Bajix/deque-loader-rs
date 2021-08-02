use crate::{task::TaskHandler, Key};
use flurry::HashMap;
use std::sync::Arc;
use tokio::sync::{oneshot, watch};

pub enum LoadState<V: Send + Sync + Clone + 'static, E: Send + Sync + Clone + 'static> {
  Ready(Result<Option<Arc<V>>, E>),
  Pending,
}

#[derive(Clone)]
pub struct WatchReceiver<V: Send + Sync + Clone + 'static, E: Send + Sync + Clone + 'static>(
  watch::Receiver<LoadState<V, E>>,
);

pub struct OneshotReceiver<V: Send + Sync + Clone + 'static, E: Send + Sync + Clone + 'static>(
  oneshot::Receiver<Result<Option<Arc<V>>, E>>,
);

impl<V, E> From<watch::Receiver<LoadState<V, E>>> for WatchReceiver<V, E>
where
  V: Send + Sync + Clone + 'static,
  E: Send + Sync + Clone + 'static,
{
  fn from(rx: watch::Receiver<LoadState<V, E>>) -> Self {
    WatchReceiver(rx)
  }
}

impl<V, E> From<oneshot::Receiver<Result<Option<Arc<V>>, E>>> for OneshotReceiver<V, E>
where
  V: Send + Sync + Clone + 'static,
  E: Send + Sync + Clone + 'static,
{
  fn from(rx: oneshot::Receiver<Result<Option<Arc<V>>, E>>) -> Self {
    OneshotReceiver(rx)
  }
}

impl<V, E> WatchReceiver<V, E>
where
  V: Send + Sync + Clone + 'static,
  E: Send + Sync + Clone + 'static,
{
  pub async fn recv(mut self) -> Result<Option<Arc<V>>, E> {
    loop {
      if let LoadState::Ready(ref result) = *self.0.borrow() {
        break result.to_owned();
      }

      self.0.changed().await.unwrap();
    }
  }
}

impl<V, E> OneshotReceiver<V, E>
where
  V: Send + Sync + Clone + 'static,
  E: Send + Sync + Clone + 'static,
{
  pub async fn recv(self) -> Result<Option<Arc<V>>, E> {
    self.0.await.unwrap()
  }
}

pub enum Request<K: Key, V: Send + Sync + Clone + 'static, E: Send + Sync + Clone + 'static> {
  Watch {
    key: K,
    tx: watch::Sender<LoadState<V, E>>,
    cache_cb: Option<Box<dyn FnOnce(&K, &V) + Send + Sync>>,
  },
  Oneshot {
    key: K,
    tx: oneshot::Sender<Result<Option<Arc<V>>, E>>,
    cache_cb: Option<Box<dyn FnOnce(&K, &V) + Send + Sync>>,
  },
}

impl<K, V, E> Request<K, V, E>
where
  K: Key,
  V: Send + Sync + Clone + 'static,
  E: Send + Sync + Clone + 'static,
{
  pub(crate) fn new_oneshot(key: K) -> (Request<K, V, E>, OneshotReceiver<V, E>) {
    let (tx, rx) = oneshot::channel();

    let request = Request::Oneshot {
      key,
      tx,
      cache_cb: None,
    };

    (request, rx.into())
  }

  pub(crate) fn new_watch(key: K) -> (Request<K, V, E>, WatchReceiver<V, E>) {
    let (tx, rx) = watch::channel(LoadState::Pending);

    let request = Request::Watch {
      key,
      tx,
      cache_cb: None,
    };

    (request, rx.into())
  }

  pub(crate) fn key(&self) -> &K {
    match self {
      Request::Watch { key, .. } => key,
      Request::Oneshot { key, .. } => key,
    }
  }

  pub(crate) fn resolve(self, value: Result<Option<Arc<V>>, E>) {
    match self {
      Request::Watch { key, tx, cache_cb } => {
        if let (Ok(Some(value)), Some(cache_cb)) = (&value, cache_cb) {
          cache_cb(&key, value);
        }

        if !tx.is_closed() {
          tx.send(LoadState::Ready(value)).ok();
        }
      }
      Request::Oneshot { key, tx, cache_cb } => {
        if let (Ok(Some(value)), Some(cache_cb)) = (&value, cache_cb) {
          cache_cb(&key, value);
        }
        if !tx.is_closed() {
          tx.send(value).ok();
        }
      }
    };
  }

  pub(crate) fn set_cache_cb(&mut self, cache_cb: Box<dyn FnOnce(&K, &V) + Send + Sync>) {
    let value = match self {
      Request::Watch { cache_cb, .. } => cache_cb,
      Request::Oneshot { cache_cb, .. } => cache_cb,
    };

    *value = Some(cache_cb);
  }
}

pub struct RequestCache<T>(LoadCache<T::Key, T::Value, T::Error>)
where
  T: TaskHandler;

impl<T> AsRef<LoadCache<T::Key, T::Value, T::Error>> for RequestCache<T>
where
  T: TaskHandler,
{
  fn as_ref(&self) -> &LoadCache<T::Key, T::Value, T::Error> {
    &self.0
  }
}

impl<T> RequestCache<T>
where
  T: TaskHandler,
{
  pub fn new() -> Self {
    RequestCache(LoadCache::new())
  }
}

pub struct LoadCache<K, V, E>
where
  K: Key,
  V: Send + Sync + Clone + 'static,
  E: Send + Sync + Clone + 'static,
{
  data: HashMap<K, watch::Receiver<LoadState<V, E>>>,
}

impl<K, V, E> LoadCache<K, V, E>
where
  K: Key,
  V: Send + Sync + Clone + 'static,
  E: Send + Sync + Clone + 'static,
{
  pub fn new() -> Self {
    LoadCache {
      data: HashMap::new(),
    }
  }

  pub(crate) fn get_or_create(&self, key: &K) -> (WatchReceiver<V, E>, Option<Request<K, V, E>>) {
    let guard = self.data.guard();

    loop {
      if let Some(rx) = self.data.get(key, &guard) {
        break (rx.clone().into(), None);
      }

      let (req, rx) = Request::new_watch(key.to_owned());

      match self.data.try_insert(key.clone(), rx.0, &guard) {
        Ok(rx) => break (rx.to_owned().into(), Some(req)),
        Err(_) => continue,
      }
    }
  }
}
