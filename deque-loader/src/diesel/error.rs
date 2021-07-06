use diesel::{
  r2d2,
  result::{ConnectionError, DatabaseErrorKind},
};
use diesel_connection::PoolError;
use thiserror::Error;

/// A combination of all [`diesel`] error types as well as the convenience types Forbidden, Unauthorized and NotFound
#[derive(Error, Debug)]
pub enum DieselError {
  #[error("Forbidden")]
  Forbidden,
  #[error("Unauthorized")]
  Unauthorized,
  #[error("Entity Not Found")]
  NotFound,
  #[error("Internal Server Error")]
  InternalServerError,
  #[error(transparent)]
  PoolError(#[from] PoolError),
  #[error(transparent)]
  ConnectionError(#[from] diesel::result::ConnectionError),
  #[error(transparent)]
  QueryError(#[from] diesel::result::Error),
}

/// Simplified [`diesel`] error types that are [`Clone`]
#[derive(Error, Debug, Clone)]
pub enum SimpleDieselError {
  #[error("Forbidden")]
  Forbidden,
  #[error("Unauthorized")]
  Unauthorized,
  #[error("Entity Not Found")]
  NotFound,
  #[error("Internal Server Error")]
  InternalServerError,
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
  #[error("Database error")]
  DatabaseError,
}

impl From<DieselError> for SimpleDieselError {
  fn from(err: DieselError) -> Self {
    match err {
      DieselError::Forbidden => SimpleDieselError::Forbidden,
      DieselError::Unauthorized => SimpleDieselError::Unauthorized,
      DieselError::NotFound => SimpleDieselError::NotFound,
      DieselError::InternalServerError => SimpleDieselError::InternalServerError,
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
      diesel::result::Error::AlreadyInTransaction => SimpleDieselError::InternalServerError,
      diesel::result::Error::DatabaseError(err, _info) => err.into(),
      diesel::result::Error::DeserializationError(_) => SimpleDieselError::InternalServerError,
      diesel::result::Error::InvalidCString(_) => SimpleDieselError::InternalServerError,
      diesel::result::Error::NotFound => SimpleDieselError::NotFound,
      diesel::result::Error::QueryBuilderError(_) => SimpleDieselError::InternalServerError,
      diesel::result::Error::SerializationError(_) => SimpleDieselError::InternalServerError,
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

#[cfg(feature = "graphql")]
impl async_graphql::ErrorExtensions for SimpleDieselError {
  fn extend(&self) -> async_graphql::FieldError {
    self.extend_with(|err, e| match err {
      SimpleDieselError::Forbidden => e.set("code", "FORBIDDEN"),
      SimpleDieselError::Unauthorized => e.set("code", "UNAUTHORIZED"),
      SimpleDieselError::NotFound => e.set("code", "NOT_FOUND"),
      SimpleDieselError::InternalServerError => e.set("code", "INTERNAL_SERVER_ERROR"),
      SimpleDieselError::BadConnection => e.set("code", "BAD_CONNECTION"),
      SimpleDieselError::InvalidConnection => e.set("code", "INVALID_CONNECTION"),
      SimpleDieselError::RollbackTransaction => e.set("code", "ROLLBACK_TRANSACTION"),
      SimpleDieselError::UniqueViolation => e.set("code", "UNIQUE_VIOLATION"),
      SimpleDieselError::ForeignKeyViolation => e.set("code", "FOREIGN_KEY_VIOLATION"),
      SimpleDieselError::ConnectionTimeout => e.set("code", "CONNECTION_TIMEOUT"),
      SimpleDieselError::DatabaseError => e.set("code", "DATABASE_ERROR"),
    })
  }
}
