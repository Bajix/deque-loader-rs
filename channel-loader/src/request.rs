use crate::key::Key;
use crate::loader::Loader;
use std::collections::HashMap;
use tokio::sync::oneshot;

pub(crate) struct Request<K: Key, T: Loader<K>> {
  pub(crate) key: K,
  tx: oneshot::Sender<Result<Option<T::Value>, T::Error>>,
}

impl<K: Key, T: Loader<K>> Request<K, T> {
  pub(crate) fn new(
    key: K,
  ) -> (
    Request<K, T>,
    oneshot::Receiver<Result<Option<T::Value>, T::Error>>,
  ) {
    let (tx, rx) = oneshot::channel();

    let request = Request { key, tx };

    (request, rx)
  }

  fn resolve(self: Self, value: Result<Option<T::Value>, T::Error>) {
    self.tx.send(value).ok();
  }
}
pub(crate) trait ResolverExt<K: Key, T: Loader<K>> {
  fn resolve(self, results: Result<HashMap<K, T::Value>, T::Error>);
}

impl<K, T> ResolverExt<K, T> for Vec<Request<K, T>>
where
  K: Key,
  T: Loader<K>,
{
  fn resolve(self, results: Result<HashMap<K, T::Value>, T::Error>) {
    match results {
      Ok(mut values) => {
        let mut iter = self.into_iter().peekable();
        while let Some(req) = iter.next() {
          // requests is already sorted by key, ergo peeking allows us to know if we can take values instead of cloning
          let can_take_value = {
            if let Some(next_req) = iter.peek() {
              !next_req.key.eq(&req.key)
            } else {
              true
            }
          };

          let value = match can_take_value {
            true => values.remove(&req.key),
            false => values.get(&req.key).map(|value| value.to_owned()),
          };

          req.resolve(Ok(value));
        }
      }
      Err(e) => {
        for req in self {
          req.resolve(Err(e.clone()));
        }
      }
    };
  }
}
