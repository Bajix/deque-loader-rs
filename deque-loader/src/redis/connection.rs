pub use redis::aio::ConnectionManager;
use redis::{Client, ErrorKind, RedisResult};
use std::env;
use tokio::sync::watch;

fn create_client() -> RedisResult<Client> {
  let database_url = env::var("REDIS_URL").unwrap_or_else(|_| {
    let hostname_env =
      env::var("REDIS_SERVICE_HOSTNAME_ENV").unwrap_or_else(|_| String::from("REDIS_HOSTNAME"));

    let port_env =
      env::var("REDIS_SERVICE_PORT_ENV").unwrap_or_else(|_| String::from("REDIS_PORT"));

    let hostname = env::var(hostname_env).unwrap_or_else(|_| String::from("127.0.0.1"));

    let port = env::var(port_env).unwrap_or_else(|_| String::from("6379"));

    match env::var("REDIS_PASSWORD") {
      Ok(password) => format!("redis://default:{}@{}:{}", password, hostname, port),
      _ => format!("redis://{}:{}", hostname, port),
    }
  });

  redis::Client::open(database_url)
}

#[derive(Clone)]
enum ManagerState {
  Initializing,
  ConnectionError(ErrorKind),
  Connected(ConnectionManager),
}

#[derive(Clone)]
struct EventualConnection {
  rx: watch::Receiver<ManagerState>,
}

impl Default for EventualConnection {
  fn default() -> Self {
    let (tx, rx) = watch::channel(ManagerState::Initializing);

    EventualConnection::initialize_connection_manager(tx);
    EventualConnection { rx }
  }
}

impl EventualConnection {
  fn initialize_connection_manager(tx: watch::Sender<ManagerState>) {
    tokio::task::spawn(async move {
      match EventualConnection::create_connection_manager().await {
        Ok(connection_manager) => tx.send(ManagerState::Connected(connection_manager)),
        Err(err) => tx.send(ManagerState::ConnectionError(err.kind())),
      }
    });
  }

  async fn create_connection_manager() -> RedisResult<ConnectionManager> {
    let client = create_client()?;
    let connection_manager = client.get_tokio_connection_manager().await?;

    Ok(connection_manager)
  }

  async fn get_connection_manager(mut self) -> Result<ConnectionManager, ErrorKind> {
    loop {
      match &*self.rx.borrow() {
        ManagerState::Initializing => {}
        ManagerState::ConnectionError(kind) => break Err(*kind),
        ManagerState::Connected(connection_manager) => break Ok(connection_manager.to_owned()),
      };

      self.rx.changed().await.unwrap();
    }
  }
}

/// thread Local multiplexed redis connection wrapped by a connection manager
pub async fn get_connection_manager() -> Result<ConnectionManager, ErrorKind> {
  thread_local! {
    static EVENTUAL_CONNECTION: EventualConnection = EventualConnection::default();
  }

  let eventual_connection = EVENTUAL_CONNECTION.with(|manager| manager.clone());

  eventual_connection.get_connection_manager().await
}
