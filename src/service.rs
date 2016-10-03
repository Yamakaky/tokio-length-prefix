use std::io;
use std::net::SocketAddr;

use futures::{Async, Future};
use futures::stream::Empty;
use proto::{self, pipeline, server};
use tokio::reactor::Handle;
use tokio_service::{Service, NewService};

use frame::length_prefix_transport;

pub struct LengthPrefixService<T> {
    inner: T,
}

impl<T> Service for LengthPrefixService<T>
where T: Service<Request = Vec<u8>, Response = Vec<u8>, Error = io::Error>,
      T::Future: 'static, {
    type Request = Vec<u8>;
    type Response = proto::Message<Vec<u8>, Empty<(), Self::Error>>;
    type Error = io::Error;
    type Future = Box<Future<Item = Self::Response, Error = Self::Error>>;

    fn call(&self, req: Vec<u8>) -> Self::Future {
        Box::new(
            self.inner
                .call(req)
                .and_then(|resp| {
                    Ok(proto::Message::WithoutBody(resp))
                })
        )
    }

    fn poll_ready(&self) -> Async<()> {
        Async::Ready(())

    }
}

pub fn serve<T>(handle: &Handle,
                addr: SocketAddr,
                new_service: T) -> io::Result<()>
where T: NewService<Request = Vec<u8>,
                    Response = Vec<u8>,
                    Error = io::Error> + Send + 'static, {
    try!(server::listen(handle, addr, move |stream| {
        let service = LengthPrefixService {
            inner: try!(new_service.new_service()),
        };
        pipeline::Server::new(service, length_prefix_transport(stream))
    }));
    Ok(())
}
