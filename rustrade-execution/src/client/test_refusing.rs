//! A local address that refuses connections, for tests of a request that fails in transport.

use std::net::SocketAddr;
use tokio::net::TcpSocket;

/// A loopback address that refuses every connection for as long as the returned socket lives.
///
/// The socket is bound but never listens, so a connect to it is refused, and its port stays
/// taken. A port bound and then released instead can be handed to a server another test starts
/// meanwhile, which would then answer the request the test means to fail.
pub(crate) fn refusing_addr() -> (TcpSocket, SocketAddr) {
    let socket = TcpSocket::new_v4().unwrap();
    socket.bind(SocketAddr::from(([127, 0, 0, 1], 0))).unwrap();
    let addr = socket.local_addr().unwrap();
    (socket, addr)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_connect_to_the_address_is_refused_while_the_socket_lives() {
        let (_socket, addr) = refusing_addr();

        let error = std::net::TcpStream::connect(addr).unwrap_err();
        assert_eq!(
            error.kind(),
            std::io::ErrorKind::ConnectionRefused,
            "{error}"
        );
    }

    #[test]
    fn the_port_stays_taken_while_the_socket_lives() {
        let (_socket, addr) = refusing_addr();

        assert!(std::net::TcpListener::bind(addr).is_err());
    }
}
