use crossbeam::atomic::AtomicCell;
use once_cell::sync::Lazy;
use redis::{Client, RedisResult};
use std::env;
use url::Url;

fn get_connection_url() -> String {
  let redis_url_env = env::var("REDIS_URL_ENV").unwrap_or_else(|_| String::from("REDIS_URL"));

  let database_url = env::var(redis_url_env).unwrap_or_else(|_| {
    let hostname = {
      let hostname_env =
        env::var("REDIS_HOSTNAME_ENV").unwrap_or_else(|_| String::from("REDIS_HOSTNAME"));

      env::var(hostname_env).unwrap_or_else(|_| String::from("127.0.0.1"))
    };

    let port = {
      let port_env = env::var("REDIS_PORT_ENV").unwrap_or_else(|_| String::from("REDIS_PORT"));

      env::var(port_env).unwrap_or_else(|_| String::from("6379"))
    };

    let database = {
      let database_env =
        env::var("REDIS_DATABASE_ENV").unwrap_or_else(|_| String::from("REDIS_DATABASE"));

      env::var(database_env).unwrap_or_else(|_| String::from("0"))
    };

    let password_env =
      env::var("REDIS_PASSWORD_ENV").unwrap_or_else(|_| String::from("REDIS_PASSWORD"));

    if let Ok(password) = env::var(password_env) {
      format!(
        "redis://default:{}@{}:{}/{}",
        password, hostname, port, database
      )
    } else {
      format!("redis://{}:{}/{}", hostname, port, database)
    }
  });

  database_url
}

fn get_database_index(database_url: &str) -> Option<i64> {
  let url = Url::parse(database_url).ok()?;
  let mut segments = url.path_segments()?;
  let database_index: i64 = segments.next()?.parse().ok()?;

  Some(database_index)
}

pub(crate) static DATABASE_INDEX: AtomicCell<i64> = AtomicCell::new(0);

pub(crate) static CLIENT: Lazy<RedisResult<Client>> = Lazy::new(|| {
  let database_url = get_connection_url();
  let database_index = get_database_index(&database_url).unwrap_or(0);

  DATABASE_INDEX.store(database_index);

  redis::Client::open(database_url)
});
