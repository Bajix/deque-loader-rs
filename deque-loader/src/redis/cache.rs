use super::{get_tracked_connection, TrackedConnection};
use crate::{
  key::Key,
  loader::{CacheStore, DataLoader, DataStore, LocalLoader},
  task::{CompletionReceipt, LoadBatch, PendingAssignment, Task, TaskAssignment, TaskHandler},
};
use log::error;
use redis::{AsyncCommands, Pipeline, RedisResult};
use serde::{de::DeserializeOwned, Serialize};
use std::{collections::HashMap, fmt::Debug, sync::Arc};
use swap_queue::Worker;
use tokio::runtime::Handle;

const REDIS_CACHE_EXPIRATION: usize = 900;

impl<K, V, E> Task<LoadBatch<K, V, E>>
where
  K: Key + CacheKey<V>,
  V: Send + Sync + Clone + 'static + Serialize,
  E: Send + Sync + Clone + 'static,
{
  pub(crate) fn update_cache_on_load(&mut self) {
    let runtime_handle = Handle::current();

    let cache_cb: Arc<(dyn Fn(&K, &V) + Send + Sync + 'static)> = Arc::new(move |k, v| {
      infallibly_update_cache(&runtime_handle, k, v);
    });

    self.0.requests.iter_mut().for_each(|req| {
      req.set_cache_cb(cache_cb.clone());
    });
  }
}
pub trait CacheKey<Value: Send + Sync + Clone + 'static>: Key + Debug {
  fn cache_key(&self) -> String;
}

impl<K, V> CacheKey<V> for K
where
  K: Key + Debug,
  V: Send + Sync + Clone + 'static + Serialize,
{
  fn cache_key(&self) -> String {
    let value_type_name: String = tynm::type_name::<V>();
    format!("{}::{{{:?}}}", value_type_name, self)
  }
}
pub struct RedisCacheAdapter<T>(T)
where
  T: LocalLoader<DataStore> + LocalLoader<CacheStore>,
  <<T as LocalLoader<DataStore>>::Handler as TaskHandler>::Key:
    CacheKey<<<T as LocalLoader<DataStore>>::Handler as TaskHandler>::Value>,
  <<T as LocalLoader<DataStore>>::Handler as TaskHandler>::Value: Serialize + DeserializeOwned;

#[async_trait::async_trait]
impl<T> TaskHandler for RedisCacheAdapter<T>
where
  T: LocalLoader<DataStore> + LocalLoader<CacheStore>,
  <<T as LocalLoader<DataStore>>::Handler as TaskHandler>::Key:
    CacheKey<<<T as LocalLoader<DataStore>>::Handler as TaskHandler>::Value>,
  <<T as LocalLoader<DataStore>>::Handler as TaskHandler>::Value: Serialize + DeserializeOwned,
{
  type Key = <<T as LocalLoader<DataStore>>::Handler as TaskHandler>::Key;
  type Value = <<T as LocalLoader<DataStore>>::Handler as TaskHandler>::Value;
  type Error = <<T as LocalLoader<DataStore>>::Handler as TaskHandler>::Error;
  const CORES_PER_WORKER_GROUP: usize =
    <<T as LocalLoader<DataStore>>::Handler as TaskHandler>::CORES_PER_WORKER_GROUP;
  const MAX_BATCH_SIZE: Option<usize> = None;

  async fn handle_task(
    task: Task<PendingAssignment<Self::Key, Self::Value, Self::Error>>,
  ) -> Task<CompletionReceipt> {
    match task.get_assignment::<Self>().await {
      TaskAssignment::LoadBatch(task) => {
        let keys = task.keys();
        let conn = get_tracked_connection();

        match RedisCacheAdapter::<T>::load(conn, keys).await {
          Ok(results) => match task.apply_partial_results(results) {
            TaskAssignment::LoadBatch(mut task) => {
              task.update_cache_on_load();
              <T as LocalLoader<DataStore>>::loader()
                .with(|loader| loader.schedule_assignment(task))
            }
            TaskAssignment::NoAssignment(receipt) => receipt,
          },
          Err(err) => {
            error!(
              "{} unable to load data from Redis: {:?}",
              tynm::type_name::<T>(),
              err
            );
            <T as LocalLoader<DataStore>>::loader().with(|loader| loader.schedule_assignment(task))
          }
        }
      }
      TaskAssignment::NoAssignment(receipt) => receipt,
    }
  }
}

impl<T> RedisCacheAdapter<T>
where
  T: LocalLoader<DataStore> + LocalLoader<CacheStore>,
  <<T as LocalLoader<DataStore>>::Handler as TaskHandler>::Key:
    CacheKey<<<T as LocalLoader<DataStore>>::Handler as TaskHandler>::Value>,
  <<T as LocalLoader<DataStore>>::Handler as TaskHandler>::Value: Serialize + DeserializeOwned,
{
  async fn load(
    mut conn: TrackedConnection,
    keys: Vec<<<T as LocalLoader<DataStore>>::Handler as TaskHandler>::Key>,
  ) -> RedisResult<
    HashMap<
      <<T as LocalLoader<DataStore>>::Handler as TaskHandler>::Key,
      Arc<<<T as LocalLoader<DataStore>>::Handler as TaskHandler>::Value>,
    >,
  > {
    let cache_keys = keys
      .iter()
      .map(|key| key.cache_key())
      .collect::<Vec<String>>();

    let results = if cache_keys.len().eq(&1) {
      let result: Option<Vec<u8>> = conn.get(cache_keys.first().unwrap()).await?;

      vec![result]
    } else {
      conn.get(cache_keys).await?
    };

    let mut data: HashMap<
      <<T as LocalLoader<DataStore>>::Handler as TaskHandler>::Key,
      Arc<<<T as LocalLoader<DataStore>>::Handler as TaskHandler>::Value>,
    > = HashMap::new();

    data.extend(
      keys
        .into_iter()
        .zip(results.iter().map(|data| match data {
          Some(data) => {
            let value: Option<<<T as LocalLoader<DataStore>>::Handler as TaskHandler>::Value> =
              bincode::deserialize(data).ok();

            value
          }
          None => None,
        }))
        .filter_map(|(key, value)| value.map(|value| (key, Arc::new(value)))),
    );

    Ok(data)
  }
}

impl<T> LocalLoader<CacheStore> for RedisCacheAdapter<T>
where
  T: LocalLoader<DataStore> + LocalLoader<CacheStore>,
  <<T as LocalLoader<DataStore>>::Handler as TaskHandler>::Key:
    CacheKey<<<T as LocalLoader<DataStore>>::Handler as TaskHandler>::Value>,
  <<T as LocalLoader<DataStore>>::Handler as TaskHandler>::Value: Serialize + DeserializeOwned,
{
  type Handler = <T as LocalLoader<CacheStore>>::Handler;
  fn loader() -> &'static std::thread::LocalKey<DataLoader<Self::Handler>> {
    <T as LocalLoader<CacheStore>>::loader()
  }
}

impl<T> LocalLoader<DataStore> for RedisCacheAdapter<T>
where
  T: LocalLoader<DataStore> + LocalLoader<CacheStore>,
  <<T as LocalLoader<DataStore>>::Handler as TaskHandler>::Key:
    CacheKey<<<T as LocalLoader<DataStore>>::Handler as TaskHandler>::Value>,
  <<T as LocalLoader<DataStore>>::Handler as TaskHandler>::Value: Serialize + DeserializeOwned,
{
  type Handler = <T as LocalLoader<DataStore>>::Handler;
  fn loader() -> &'static std::thread::LocalKey<DataLoader<Self::Handler>> {
    <T as LocalLoader<DataStore>>::loader()
  }
}

/// Update cache if able or fail silently
pub fn infallibly_update_cache<K, V>(runtime_handle: &Handle, key: &K, value: &V)
where
  K: Key + CacheKey<V>,
  V: Send + Sync + Clone + 'static + Serialize,
{
  thread_local! {
    static QUEUE: Worker<(String, Vec<u8>)> = Worker::new();
  }

  match bincode::serialize(value) {
    Ok(data) => {
      QUEUE.with(|queue| {
        if let Some(stealer) = queue.push((key.cache_key(), data)) {
          runtime_handle.spawn(async move {
            let mset = stealer.take().await;

            let mut conn = get_tracked_connection();

            let mut pipeline = Pipeline::new();

            for (key, value) in mset.into_iter() {
              pipeline.set_ex(key, value, REDIS_CACHE_EXPIRATION);
            }

            let result: RedisResult<()> = pipeline.query_async(&mut conn).await;

            if let Err(err) = result {
              error!("Unable to store {}: {:?}", tynm::type_name::<V>(), err);
            }
          });
        }
      });
    }
    Err(err) => {
      error!(
        "{} cannot bincode::serialize: {:?}",
        tynm::type_name::<V>(),
        err
      );
    }
  }
}
