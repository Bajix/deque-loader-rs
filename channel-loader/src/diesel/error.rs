use diesel::{
  r2d2,
  result::{ConnectionError, DatabaseErrorKind},
};
use diesel_connection::PoolError;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum DieselError {
  #[error("Forbidden")]
  Forbidden,
  #[error("Unauthorized")]
  Unauthorized,
  #[error("Entity Not Found")]
  NotFound,
  #[error(transparent)]
  PoolError(#[from] PoolError),
  #[error(transparent)]
  ConnectionError(#[from] diesel::result::ConnectionError),
  #[error(transparent)]
  QueryError(#[from] diesel::result::Error),
}

#[derive(Error, Debug, Clone)]
pub enum SimpleDieselError {
  #[error("Forbidden")]
  Forbidden,
  #[error("Unauthorized")]
  Unauthorized,
  #[error("Entity Not Found")]
  NotFound,
  #[error("Bad connection")]
  BadConnection,
  #[error("Invalid connection")]
  InvalidConnection,
  #[error("Transaction conflict")]
  RollbackTransaction,
  #[error("Unique key violation")]
  UniqueViolation,
  #[error("Foreign key violation")]
  ForeignKeyViolation,
  #[error("Connection timed out")]
  ConnectionTimeout,
  #[error("Internal database error")]
  DatabaseError,
}

impl From<DieselError> for SimpleDieselError {
  fn from(err: DieselError) -> Self {
    match err {
      DieselError::Forbidden => SimpleDieselError::Forbidden,
      DieselError::Unauthorized => SimpleDieselError::Unauthorized,
      DieselError::NotFound => SimpleDieselError::NotFound,
      DieselError::PoolError(err) => err.into(),
      DieselError::ConnectionError(err) => err.into(),
      DieselError::QueryError(err) => err.into(),
    }
  }
}

impl From<PoolError> for SimpleDieselError {
  fn from(_: PoolError) -> Self {
    SimpleDieselError::ConnectionTimeout
  }
}

impl From<r2d2::Error> for SimpleDieselError {
  fn from(err: r2d2::Error) -> Self {
    match err {
      r2d2::Error::ConnectionError(err) => err.into(),
      r2d2::Error::QueryError(_) => SimpleDieselError::ConnectionTimeout,
    }
  }
}

impl From<ConnectionError> for SimpleDieselError {
  fn from(err: ConnectionError) -> Self {
    match err {
      ConnectionError::BadConnection(_) => SimpleDieselError::BadConnection,
      _ => SimpleDieselError::InvalidConnection,
    }
  }
}

impl From<diesel::result::Error> for SimpleDieselError {
  fn from(err: diesel::result::Error) -> Self {
    match err {
      diesel::result::Error::AlreadyInTransaction => SimpleDieselError::DatabaseError,
      diesel::result::Error::DatabaseError(err, _info) => err.into(),
      diesel::result::Error::DeserializationError(_) => SimpleDieselError::DatabaseError,
      diesel::result::Error::InvalidCString(_) => SimpleDieselError::DatabaseError,
      diesel::result::Error::NotFound => SimpleDieselError::NotFound,
      diesel::result::Error::QueryBuilderError(_) => SimpleDieselError::DatabaseError,
      diesel::result::Error::SerializationError(_) => SimpleDieselError::DatabaseError,
      _ => SimpleDieselError::DatabaseError,
    }
  }
}

impl From<DatabaseErrorKind> for SimpleDieselError {
  fn from(err: DatabaseErrorKind) -> Self {
    match err {
      DatabaseErrorKind::ForeignKeyViolation => SimpleDieselError::ForeignKeyViolation,
      DatabaseErrorKind::SerializationFailure => SimpleDieselError::DatabaseError,
      DatabaseErrorKind::UnableToSendCommand => SimpleDieselError::DatabaseError,
      DatabaseErrorKind::UniqueViolation => SimpleDieselError::UniqueViolation,
      _ => SimpleDieselError::DatabaseError,
    }
  }
}
