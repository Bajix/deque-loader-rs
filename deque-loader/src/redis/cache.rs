use super::{get_connection_manager, ConnectionManager};
use crate::{
  key::Key,
  loader::{CacheStore, DataLoader, DataStore, LocalLoader},
  task::{CompletionReceipt, LoadBatch, PendingAssignment, Task, TaskAssignment, TaskHandler},
};
use crossbeam::{
  atomic::AtomicCell,
  deque::{Steal, Worker},
};
use log::error;
use redis::{AsyncCommands, Pipeline, RedisResult};
use serde::{de::DeserializeOwned, Serialize};
use std::{collections::HashMap, fmt::Debug, sync::Arc};
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
    let conn = get_connection_manager().await;

    match task.get_assignment::<Self>() {
      TaskAssignment::LoadBatch(task) => match conn {
        Ok(conn) => {
          let keys = task.keys();

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
              <T as LocalLoader<DataStore>>::loader()
                .with(|loader| loader.schedule_assignment(task))
            }
          }
        }
        Err(err) => {
          error!(
            "{} unable to acquire Redis connection: {:?}",
            tynm::type_name::<T>(),
            err
          );
          <T as LocalLoader<DataStore>>::loader().with(|loader| loader.schedule_assignment(task))
        }
      },
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
    mut conn: ConnectionManager,
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
    static QUEUE: (Worker<(String, Vec<u8>)>, Arc<AtomicCell<usize>>) = (Worker::new_fifo(), Arc::new(AtomicCell::new(0)));
  }

  match bincode::serialize(value) {
    Ok(data) => {
      QUEUE.with(|(queue, queue_size)| {
        if queue_size.fetch_add(1).eq(&0) {
          let stealer = queue.stealer();

          let queue_size = queue_size.to_owned();
          runtime_handle.spawn(async move {
            let mut mset = vec![];

            loop {
              mset.extend(std::iter::from_fn(|| loop {
                match stealer.steal() {
                  Steal::Success(set_key_value) => break Some(set_key_value),
                  Steal::Retry => continue,
                  Steal::Empty => break None,
                }
              }));

              if queue_size.compare_exchange(mset.len(), 0).is_ok() {
                break;
              }
            }

            match get_connection_manager().await {
              Ok(mut conn) => {
                let mut pipeline = Pipeline::new();

                for (key, value) in mset.into_iter() {
                  pipeline.set_ex(key, value, REDIS_CACHE_EXPIRATION);
                }

                let result: RedisResult<()> = pipeline.query_async(&mut conn).await;

                if let Err(err) = result {
                  error!("Unable to store {}: {:?}", tynm::type_name::<V>(), err);
                }
              }
              Err(err) => {
                error!("Unable to acquire Redis connection: {:?}", err);
              }
            }
          });
        }

        queue.push((key.cache_key(), data));
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
