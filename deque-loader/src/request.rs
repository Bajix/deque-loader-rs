use crate::{task::TaskHandler, Key};
use flurry::HashMap;
use std::{collections::HashSet, sync::Arc, vec::IntoIter};
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
    cache_cb: Option<Arc<dyn Fn(&K, &V) + Send + Sync>>,
  },
  Oneshot {
    key: K,
    tx: oneshot::Sender<Result<Option<Arc<V>>, E>>,
    cache_cb: Option<Arc<dyn Fn(&K, &V) + Send + Sync>>,
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

  pub(crate) fn key<'a>(&'a self) -> &'a K {
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

  pub(crate) fn set_cache_cb(&mut self, cache_cb: Arc<dyn Fn(&K, &V) + Send + Sync>) {
    let value = match self {
      Request::Watch { cache_cb, .. } => cache_cb,
      Request::Oneshot { cache_cb, .. } => cache_cb,
    };

    *value = Some(cache_cb);
  }
}

struct RequestBucket<K: Key, V: Send + Sync + Clone + 'static, E: Send + Sync + Clone + 'static> {
  keys: HashSet<K>,
  requests: Vec<Request<K, V, E>>,
}

impl<K, V, E> Default for RequestBucket<K, V, E>
where
  K: Key,
  V: Send + Sync + Clone + 'static,
  E: Send + Sync + Clone + 'static,
{
  fn default() -> Self {
    let keys = HashSet::new();
    let requests = vec![];
    RequestBucket { keys, requests }
  }
}

impl<K, V, E> RequestBucket<K, V, E>
where
  K: Key,
  V: Send + Sync + Clone + 'static,
  E: Send + Sync + Clone + 'static,
{
  fn insert(&mut self, req: Request<K, V, E>) {
    self.keys.insert(req.key().to_owned());
    self.requests.push(req);
  }
}

pub(crate) struct RequestBuckets<
  K: Key,
  V: Send + Sync + Clone + 'static,
  E: Send + Sync + Clone + 'static,
> {
  buckets: Vec<RequestBucket<K, V, E>>,
  bucket_capacity: usize,
}

impl<K, V, E> RequestBuckets<K, V, E>
where
  K: Key,
  V: Send + Sync + Clone + 'static,
  E: Send + Sync + Clone + 'static,
{
  pub(crate) fn new(bucket_capacity: usize) -> Self {
    RequestBuckets {
      buckets: vec![],
      bucket_capacity,
    }
  }

  fn insert(&mut self, req: Request<K, V, E>) {
    let bucket_capacity = self.bucket_capacity;

    if let Some(bucket) = self
      .buckets
      .iter_mut()
      .find(|bucket| bucket.keys.len() < bucket_capacity || bucket.keys.contains(req.key()))
    {
      bucket.insert(req);
    } else {
      let mut bucket = RequestBucket::default();
      bucket.insert(req);

      self.buckets.push(bucket);
    }
  }
}

impl<'a, K, V, E> Extend<Request<K, V, E>> for RequestBuckets<K, V, E>
where
  K: Key,
  V: Send + Sync + Clone + 'static,
  E: Send + Sync + Clone + 'static,
{
  fn extend<T>(&mut self, iter: T)
  where
    T: IntoIterator<Item = Request<K, V, E>>,
  {
    for req in iter {
      self.insert(req);
    }
  }
}

impl<K, V, E> IntoIterator for RequestBuckets<K, V, E>
where
  K: Key,
  V: Send + Sync + Clone + 'static,
  E: Send + Sync + Clone + 'static,
{
  type Item = Vec<Request<K, V, E>>;
  type IntoIter = IntoIter<Vec<Request<K, V, E>>>;
  fn into_iter(self) -> <Self as IntoIterator>::IntoIter {
    let buckets: Vec<Vec<Request<K, V, E>>> =
      self.buckets.into_iter().map(|req| req.requests).collect();

    buckets.into_iter()
  }
}

pub struct LoadCache<T>
where
  T: TaskHandler,
{
  data: HashMap<T::Key, watch::Receiver<LoadState<T::Value, T::Error>>>,
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

  pub(crate) fn get_or_create(
    &self,
    key: &T::Key,
  ) -> (
    WatchReceiver<T::Value, T::Error>,
    Option<Request<T::Key, T::Value, T::Error>>,
  ) {
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
