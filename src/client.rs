use std::io;
use std::net::SocketAddr;

use proto::{self, pipeline};
use futures::{Async, Future};
use futures::stream::Empty;
use tokio::net::TcpStream;
use tokio::reactor::Handle;
use tokio_service::Service;

use frame::length_prefix_transport;

/// And the client handle.
pub struct Client {
    inner: proto::Client<Vec<u8>,
                         Vec<u8>,
                         Empty<(), io::Error>,
                         io::Error>,
}

impl Service for Client {
    type Request = Vec<u8>;
    type Response = Vec<u8>;
    type Error = io::Error;
    type Future = Box<Future<Item = Self::Response,
                             Error = Self::Error>>;

    fn call(&self, req: Vec<u8>) -> Self::Future {
        self.inner
            .call(proto::Message::WithoutBody(req))
            .boxed()
    }

    fn poll_ready(&self) -> Async<()> {
        Async::Ready(())
    }
}

pub fn connect(handle: Handle, addr: &SocketAddr) -> Client {
    let addr = addr.clone();
    let h = handle.clone();

    let new_transport = move || {
        TcpStream::connect(&addr, &h).map(length_prefix_transport)
    };

    // Connect the client
    let client = pipeline::connect(new_transport, &handle);
    Client { inner: client }
}
