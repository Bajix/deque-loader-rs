use async_graphql::{
  extensions::ApolloTracing,
  http::{playground_source, GraphQLPlaygroundConfig},
};
use async_graphql_warp::{BadRequest, Response};
use channel_loader::graphql::insert_loader_caches;
use http::StatusCode;
use schema::{EmptySubscription, MutationRoot, QueryRoot, Schema};
use std::convert::Infallible;
use warp::{http::Response as HttpResponse, Filter, Rejection};

#[macro_use]
extern crate diesel;
#[macro_use]
extern crate derive_id;
#[macro_use]
extern crate channel_loader;

extern crate log;
extern crate pretty_env_logger;

mod data;
mod schema;

#[tokio::main]
async fn main() {
  dotenv::dotenv().expect("Unable to find .env file");
  pretty_env_logger::init();
  booter::boot();

  let schema: Schema<QueryRoot, MutationRoot, EmptySubscription> =
    Schema::build(QueryRoot, MutationRoot, EmptySubscription)
      .extension(ApolloTracing)
      .finish();

  let graphql_post = async_graphql_warp::graphql(schema).and_then(
    |(schema, mut request): (
      Schema<QueryRoot, MutationRoot, EmptySubscription>,
      async_graphql::Request,
    )| async move {
      request = insert_loader_caches(request);

      Ok::<_, Infallible>(Response::from(schema.execute(request).await))
    },
  );

  let graphql_playground = warp::path::end().and(warp::get()).map(|| {
    HttpResponse::builder()
      .header("content-type", "text/html")
      .body(playground_source(GraphQLPlaygroundConfig::new("/")))
  });

  let routes = graphql_playground
    .or(graphql_post)
    .recover(|err: Rejection| async move {
      if let Some(BadRequest(err)) = err.find() {
        return Ok::<_, Infallible>(warp::reply::with_status(
          err.to_string(),
          StatusCode::BAD_REQUEST,
        ));
      }

      Ok(warp::reply::with_status(
        "INTERNAL_SERVER_ERROR".to_string(),
        StatusCode::INTERNAL_SERVER_ERROR,
      ))
    });

  warp::serve(routes).run(([0, 0, 0, 0], 3000)).await;
}
