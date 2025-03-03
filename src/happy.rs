use std::io;
use std::iter::{self, FusedIterator};
use std::net::{IpAddr, SocketAddr, TcpStream, ToSocketAddrs};
use std::sync::mpsc::channel;
use std::thread;
use std::time::{Duration, Instant};

use socket2::{Domain, Socket, Type};
use url::Host;

const RACE_DELAY: Duration = Duration::from_millis(200);

/// Attempts to connect to the specified socket address within a given timeout, optionally binding to a local address.
///
/// # Arguments
///
/// * `addr` - The remote socket address to connect to.
/// * `timeout` - The duration within which the connection attempt should complete.
/// * `bind_to` - An optional local socket address to bind to before connecting.
///
/// # Returns
///
/// * `Ok(TcpStream)` - If the connection is successful.
/// * `Err(io::Error)` - If the connection fails or times out.
fn connect_timeout_bind(addr: &SocketAddr, timeout: Duration, bind_to: &Option<SocketAddr>) -> io::Result<TcpStream> {
    let domain = Domain::for_address(*addr);
    let socket = Socket::new(domain, Type::STREAM, None)?;
    if let Some(bind_to) = bind_to {
        socket.bind(&(*bind_to).into())?;
    }
    socket.connect_timeout(&(*addr).into(), timeout)?;

    Ok(socket.into())
}

/// This function implements a basic form of the happy eyeballs RFC to quickly connect
/// to a domain which is available in both IPv4 and IPv6. Connection attempts are raced
/// against each other and the first to connect successfully wins the race.
pub fn connect(
    host: &Host<&str>,
    port: u16,
    timeout: Duration,
    deadline: Option<Instant>,
    bind_to: &Option<SocketAddr>,
) -> io::Result<TcpStream> {
    let addrs: Vec<_> = match *host {
        Host::Domain(domain) => (domain, port).to_socket_addrs()?.collect(),
        Host::Ipv4(ip) => return connect_timeout_bind(&(IpAddr::V4(ip), port).into(), timeout, bind_to),
        Host::Ipv6(ip) => return connect_timeout_bind(&(IpAddr::V6(ip), port).into(), timeout, bind_to),
    };

    if let [addr] = &addrs[..] {
        debug!("DNS returned only one address, using fast path");
        return connect_timeout_bind(addr, timeout, bind_to);
    }

    let ipv4 = addrs.iter().filter(|a| a.is_ipv4());
    let ipv6 = addrs.iter().filter(|a| a.is_ipv6());
    let sorted = intertwine(ipv6, ipv4);

    let (tx, rx) = channel();
    let mut first_err = None;

    let start = Instant::now();

    let mut handle_res = |addr, res| match res {
        Ok(sock) => {
            debug!(
                "successfully connected to {}, took {}ms",
                addr,
                start.elapsed().as_millis()
            );

            Some(sock)
        }
        Err(err) => {
            debug!("failed to connect to {}: {}", addr, err);

            if first_err.is_none() {
                first_err = Some(err);
            }

            None
        }
    };

    // This loop will race each connection attempt against others, returning early if a
    // connection attempt is successful.
    for &addr in sorted {
        let tx = tx.clone();
        let bind_to = *bind_to;

        thread::spawn(move || {
            debug!("trying to connect to {}", addr);

            let res = match deadline.map(|deadline| deadline.checked_duration_since(Instant::now())) {
                None => connect_timeout_bind(&addr, timeout, &bind_to),
                Some(Some(timeout1)) => connect_timeout_bind(&addr, timeout.min(timeout1), &bind_to),
                Some(None) => Err(io::ErrorKind::TimedOut.into()),
            };

            let _ = tx.send((addr, res));
        });

        if let Ok((addr, res)) = rx.recv_timeout(RACE_DELAY) {
            if let Some(sock) = handle_res(addr, res) {
                return Ok(sock);
            }
        }
    }

    // We must drop this handle to the sender in order to properly disconnect the channel
    // when all the threads are finished.
    drop(tx);

    // This loop waits for replies from the background threads. It will automatically timeout
    // when the background threads' connection attempts timeout and the senders are dropped.
    // This loop is reached when some of the threads do not complete within the race delay.
    for (addr, res) in rx.iter() {
        if let Some(sock) = handle_res(addr, res) {
            return Ok(sock);
        }
    }

    debug!(
        "could not connect to any address, took {}ms",
        start.elapsed().as_millis()
    );

    Err(first_err.unwrap_or_else(|| io::Error::new(io::ErrorKind::Other, "no DNS entries found")))
}

fn intertwine<T, A, B>(mut ita: A, mut itb: B) -> impl Iterator<Item = T>
where
    A: FusedIterator<Item = T>,
    B: FusedIterator<Item = T>,
{
    let mut valb = None;

    iter::from_fn(move || {
        if let Some(b) = valb.take() {
            return Some(b);
        }

        match (ita.next(), itb.next()) {
            (Some(a), Some(b)) => {
                valb = Some(b);
                Some(a)
            }
            (Some(a), None) => Some(a),
            (None, Some(b)) => Some(b),
            (None, None) => None,
        }
    })
}

#[test]
fn test_intertwine_even() {
    let x: Vec<u32> = intertwine(vec![1, 2, 3].into_iter(), vec![4, 5, 6].into_iter()).collect();
    assert_eq!(&x[..], &[1, 4, 2, 5, 3, 6][..]);
}

#[test]
fn test_intertwine_left() {
    let x: Vec<u32> = intertwine(vec![1, 2, 3, 100, 101].into_iter(), vec![4, 5, 6].into_iter()).collect();
    assert_eq!(&x[..], &[1, 4, 2, 5, 3, 6, 100, 101][..]);
}

#[test]
fn test_intertwine_right() {
    let x: Vec<u32> = intertwine(vec![1, 2, 3].into_iter(), vec![4, 5, 6, 100, 101].into_iter()).collect();
    assert_eq!(&x[..], &[1, 4, 2, 5, 3, 6, 100, 101][..]);
}
