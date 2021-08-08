extern crate darling;
extern crate syn;

use darling::FromDeriveInput;
use proc_macro2::TokenStream;
use quote::quote;
use syn::{parse_macro_input, DeriveInput};

#[derive(Debug, FromDeriveInput)]
#[darling(attributes(data_loader))]
struct LoaderArgs {
  #[darling(multiple)]
  handler: Vec<syn::Path>,
}

#[derive(Debug, FromDeriveInput)]
#[darling(attributes(cached_loader))]
struct CachedLoaderArgs {
  #[darling(multiple)]
  handler: Vec<syn::Path>,
}

#[proc_macro_derive(Loadable, attributes(data_loader, cached_loader))]
pub fn load_by(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
  let input = parse_macro_input!(input as DeriveInput);

  let loader_args: LoaderArgs =
    FromDeriveInput::from_derive_input(&input).expect("can't parse data_loader attribute");
  let cached_loader_args: CachedLoaderArgs =
    FromDeriveInput::from_derive_input(&input).expect("can't parse cached_loader attribute");

  let loadable = &input.ident;

  let handler = &loader_args.handler;

  let cached_handler: Vec<TokenStream> = cached_loader_args
    .handler
    .iter()
    .map(|handler| {
      quote! {
        deque_loader::redis::CacheHandler<#handler>
      }
    })
    .collect();

  let expanded = quote! {
    #(
      #[deque_loader::async_trait::async_trait]
      impl deque_loader::loadable::LoadBy<
          #handler,
          <#handler as deque_loader::task::TaskHandler>::Key,
          <#handler as deque_loader::task::TaskHandler>::Value,
        > for #loadable
      {
        type Error = <#handler as deque_loader::task::TaskHandler>::Error;
        async fn load_by(key: <#handler as deque_loader::task::TaskHandler>::Key) -> Result<Option<std::sync::Arc<<#handler as deque_loader::task::TaskHandler>::Value>>, Self::Error> {
          let rx = <#handler as deque_loader::loader::LocalLoader<deque_loader::loader::DataStore>>::loader().with(|loader| loader.load_by(key));

          rx.recv().await
        }

        async fn cached_load_by<RequestCache: Send + Sync + AsRef<deque_loader::request::LoadCache<#handler>>>(
          key: <#handler as deque_loader::task::TaskHandler>::Key,
          request_cache: &RequestCache
        ) -> Result<Option<std::sync::Arc<<#handler as deque_loader::task::TaskHandler>::Value>>, Self::Error> {
          let rx =
            <#handler as deque_loader::loader::LocalLoader<deque_loader::loader::DataStore>>::loader().with(|loader| loader.cached_load_by(key, request_cache));

          rx.recv().await
        }
      }
    )
    *
    #(
      #[deque_loader::async_trait::async_trait]
      impl deque_loader::loadable::LoadBy<
          #cached_handler,
          <#cached_handler as deque_loader::task::TaskHandler>::Key,
          <#cached_handler as deque_loader::task::TaskHandler>::Value,
        > for #loadable
      {
        type Error = <#cached_handler as deque_loader::task::TaskHandler>::Error;
        async fn load_by(key: <#cached_handler as deque_loader::task::TaskHandler>::Key) -> Result<Option<std::sync::Arc<<#cached_handler as deque_loader::task::TaskHandler>::Value>>, Self::Error> {
          let rx = <#cached_handler as deque_loader::loader::LocalLoader<deque_loader::loader::CacheStore>>::loader().with(|loader| loader.load_by(key));

          rx.recv().await
        }

        async fn cached_load_by<RequestCache: Send + Sync + AsRef<deque_loader::request::LoadCache<#cached_handler>>>(
          key: <#cached_handler as deque_loader::task::TaskHandler>::Key,
          request_cache: &RequestCache
        ) -> Result<Option<std::sync::Arc<<#cached_handler as deque_loader::task::TaskHandler>::Value>>, Self::Error> {
          let rx =
            <#cached_handler as deque_loader::loader::LocalLoader<deque_loader::loader::CacheStore>>::loader().with(|loader| loader.cached_load_by(key, request_cache));

          rx.recv().await
        }
      }
    )
    *
  };

  proc_macro::TokenStream::from(expanded)
}

#[proc_macro_derive(Loader, attributes(data_loader, cached_loader))]
pub fn local_loader(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
  let input = parse_macro_input!(input as DeriveInput);

  let loader_args: LoaderArgs =
    FromDeriveInput::from_derive_input(&input).expect("can't parse data_loader attribute");
  let cached_loader_args: CachedLoaderArgs =
    FromDeriveInput::from_derive_input(&input).expect("can't parse cached_loader attribute");

  let handler: Vec<&syn::Path> = loader_args
    .handler
    .iter()
    .chain(cached_loader_args.handler.iter())
    .collect();

  let cached_handler: Vec<TokenStream> = cached_loader_args
    .handler
    .iter()
    .map(|handler| {
      quote! {
        deque_loader::redis::CacheHandler<#handler>
      }
    })
    .collect();

  let loader = input.ident;

  let expanded = quote! {
    #(
      impl deque_loader::loader::LocalLoader<deque_loader::loader::DataStore> for #loader {
        type Handler = #handler;
        fn loader() -> &'static std::thread::LocalKey<deque_loader::loader::DataLoader<Self::Handler>> {
          #[static_init::dynamic(0)]
          static WORKER_REGISTRY: deque_loader::worker::WorkerRegistry<#handler> = deque_loader::worker::WorkerRegistry::new();

          thread_local! {
            static DATA_LOADER: deque_loader::loader::DataLoader<#handler> = deque_loader::loader::DataLoader::from_registry(unsafe { &WORKER_REGISTRY });
          }

          &DATA_LOADER
        }
      }
    )
    *
    #(
      impl deque_loader::loader::LocalLoader<deque_loader::loader::CacheStore> for #loader {
        type Handler = #cached_handler;
        fn loader() -> &'static std::thread::LocalKey<deque_loader::loader::DataLoader<Self::Handler>> {
          #[static_init::dynamic(0)]
          static WORKER_REGISTRY: deque_loader::worker::WorkerRegistry<#cached_handler> = deque_loader::worker::WorkerRegistry::new();

          thread_local! {
            static DATA_LOADER: deque_loader::loader::DataLoader<#cached_handler> = deque_loader::loader::DataLoader::from_registry(unsafe { &WORKER_REGISTRY });
          }

          &DATA_LOADER
        }
      }
    )
    *
  };

  proc_macro::TokenStream::from(expanded)
}
