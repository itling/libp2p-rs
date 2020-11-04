// Copyright 2020 Netwarps Ltd.
//
// Permission is hereby granted, free of charge, to any person obtaining a
// copy of this software and associated documentation files (the "Software"),
// to deal in the Software without restriction, including without limitation
// the rights to use, copy, modify, merge, publish, distribute, sublicense,
// and/or sell copies of the Software, and to permit persons to whom the
// Software is furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
// OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
// FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
// DEALINGS IN THE SOFTWARE.

use crate::connection::{Connection, Direction};
use crate::{SwarmError, SwarmEvent};
use async_std::sync::Arc;
use async_std::{future, task};
use fnv::FnvHashMap;
use futures::channel::{mpsc, oneshot};
use futures::lock::Mutex;
use futures::prelude::*;
use futures_timer::Delay;
use libp2prs_core::muxing::StreamMuxerEx;
use libp2prs_core::peerstore::PeerStore;
use libp2prs_core::transport::upgrade::ITransportEx;
use libp2prs_core::transport::TransportError;
use libp2prs_core::{
    multiaddr::{protocol, Multiaddr},
    PeerId,
};
use std::sync::atomic::Ordering;
use std::sync::atomic::{AtomicBool, AtomicU32};
use std::time::{Duration, Instant};

type Result<T> = std::result::Result<T, SwarmError>;

/// CONCURRENT_DIALS_LIMIT  is the number of concurrent outbound dials
const CONCURRENT_DIALS_LIMIT: u32 = 100;

/// DIAL_TIMEOUT is the maximum duration a Dial is allowed to take.This includes the time between dialing the raw network connection,protocol selection as well the handshake, if applicable.
const DIAL_TIMEOUT: Duration = Duration::from_secs(60);

/// DIAL_TIMEOUT_LOCAL is the maximum duration a Dial to local network address is allowed to take.This includes the time between dialing the raw network connection,protocol selection as well the handshake, if applicable.
const DIAL_TIMEOUT_LOCAL: Duration = Duration::from_secs(5);

/// DIAL_ATTEMPTS is the maximum dial attempts (default: 0).
const DIAL_ATTEMPTS: u32 = 0;

/// BACKOFF_BASE is the base amount of time to backoff (default: 5s).
const BACKOFF_BASE: Duration = Duration::from_secs(5);

/// BACKOFF_COEF is the backoff coefficient (default: 1s).
const BACKOFF_COEF: Duration = Duration::from_secs(1);

/// BACKOFF_MAX is the maximum backoff time (default: 300s).
const BACKOFF_MAX: Duration = Duration::from_secs(300);

#[derive(Clone)]
struct DialJob {
    addr: Multiaddr,
    peer: PeerId,
    reply: mpsc::UnboundedSender<Result<Box<dyn StreamMuxerEx>>>,
    transport: ITransportEx,
    is_cancel: Arc<AtomicBool>,
}
#[derive(Clone)]
pub(crate) struct DialLimiter {
    dial_consuming: Arc<AtomicU32>,
    dial_limit: u32,
}

impl Default for DialLimiter {
    fn default() -> Self {
        Self::new()
    }
}

impl DialLimiter {
    fn new() -> Self {
        let mut dial_limit = CONCURRENT_DIALS_LIMIT;
        for (key, value) in std::env::vars() {
            if key == "LIBP2P_SWARM_DIAL_LIMIT" {
                dial_limit = value.parse::<u32>().unwrap();
            }
        }
        DialLimiter {
            dial_consuming: Arc::new(AtomicU32::new(0)),
            dial_limit,
        }
    }

    fn dial_timeout(&self, ma: &Multiaddr) -> Duration {
        let mut timeout: Duration = DIAL_TIMEOUT;
        if ma.is_private_addr() {
            timeout = DIAL_TIMEOUT_LOCAL;
        }
        timeout
    }

    /// add_dial_job tries to take the needed tokens for starting the given dial job.
    async fn add_dial_job(&self, mut dj: DialJob) {
        log::debug!("[DialLimiter] executing a dial job through limiter: {}", &dj.addr);
        if self.dial_consuming.load(Ordering::SeqCst) >= self.dial_limit {
            log::info!(
                "[DialLimiter] Terminate dial waiting on dial token; peer: {}; addr: {}; consuming: {:?}; limit: {:?};",
                &dj.peer,
                &dj.addr,
                self.dial_consuming,
                self.dial_limit,
            );
            let _ = dj.reply.send(Err(SwarmError::ConcurrentDialLimit(self.dial_limit))).await;
            return;
        }
        log::trace!(
            "[DialLimiter] taking dial token: peer: {}; addr: {}; prev consuming: {:?}",
            &dj.peer,
            &dj.addr,
            self.dial_consuming
        );
        self.dial_consuming.fetch_add(1, Ordering::SeqCst);
        self.execute_dial(dj).await;
    }

    /// execute_dial calls the do_dial method to dial, and reports the result through the response
    /// channel when finished. Once the response is sent it also releases all tokens
    /// it held during the dial.
    async fn execute_dial(&self, dj: DialJob) {
        let addr = dj.addr.clone();
        let mut job_reply = dj.clone().reply;
        let timeout = self.dial_timeout(&addr);
        let dial_r = future::timeout(timeout, self.do_dial(dj)).await;
        if let Ok(r) = dial_r {
            let _ = job_reply.send(r).await;
        } else {
            let _ = job_reply.send(Err(SwarmError::DialTimeout(addr, timeout.as_secs()))).await;
        }
        self.dial_consuming.fetch_sub(1, Ordering::SeqCst);
    }

    /// Tries to dial the given address.
    async fn do_dial(&self, dj: DialJob) -> Result<Box<dyn StreamMuxerEx>> {
        let addr = dj.addr.clone();
        let peer_id = dj.peer.clone();
        log::debug!("[DialLimiter] dialing addr={:?}, expecting {:?}", &addr, &peer_id);
        if dj.is_cancel.load(Ordering::SeqCst) {
            log::debug!(
                "[DialLimiter] Cancel the current task because another task has already succeeded, addr={}",
                &addr
            );
            return Err(SwarmError::DialCancelled(addr));
        }
        //let mut tx = job.event_sender.clone();
        let mut transport = dj.transport.clone();
        let r = transport.dial(addr.clone()).await;
        r.map_err(|err| SwarmError::TransportDialFailed(addr, err))
    }
}

/// DialBackoff is a type for tracking peer dial backoffs.
//
// * It's thread-safe.
#[derive(Clone)]
pub(crate) struct DialBackoff {
    entries: Arc<Mutex<FnvHashMap<PeerId, FnvHashMap<String, BackoffAddr>>>>,
    max_time: Option<Duration>,
}

#[derive(Clone, Debug)]
struct BackoffAddr {
    tries: u32,
    until: Instant,
}

impl Default for DialBackoff {
    fn default() -> Self {
        Self::new()
    }
}

#[allow(dead_code)]
impl DialBackoff {
    pub fn new() -> Self {
        Self {
            entries: Default::default(),
            max_time: None,
        }
    }

    pub fn with_max_time(mut self, time: Duration) -> Self {
        self.max_time = Some(time);
        self
    }

    /// Backoff returns whether the client should backoff from dialing peer p at address addr
    pub async fn is_backoff(&self, peer_id: PeerId, ma: Multiaddr) -> bool {
        log::trace!("[DialBackoff] is backoff,addr={:?}", ma);
        let lock = self.entries.lock().await;
        if let Some(peer_map) = lock.get(&peer_id) {
            if let Some(backoff) = peer_map.get(&ma.to_string()) {
                log::trace!(
                    "[DialBackoff] is backoff: Instant::now() ={:?},ma={:?},backoff={:?}",
                    Instant::now(),
                    &ma,
                    backoff
                );
                return Instant::now() < backoff.until;
            }
        }
        false
    }

    /// add_backoff lets other nodes know that we've entered backoff with peer p, so dialers should not wait unnecessarily.
    /// We still will attempt to dial with task::spawn, in case we get through.
    ///
    /// Backoff is not exponential, it's quadratic and computed according to the following formula:
    ///
    /// BackoffBase + BakoffCoef * PriorBackoffs^2
    ///
    /// Where PriorBackoffs is the number of previous backoffs.
    pub async fn add_backoff(&self, peer_id: PeerId, ma: Multiaddr) {
        let mut lock = self.entries.lock().await;
        let peer_map = lock.entry(peer_id.clone()).or_insert_with(Default::default);
        if let Some(backoff) = peer_map.get_mut(&ma.to_string()) {
            let mut backoff_time = BACKOFF_BASE + BACKOFF_COEF * (backoff.tries * backoff.tries);
            if backoff_time > BACKOFF_MAX {
                backoff_time = BACKOFF_MAX
            }
            backoff.until = Instant::now() + backoff_time;
            backoff.tries += 1;
            log::trace!("[DialBackoff] init backoff,{:?}", backoff);
        } else {
            let until = Instant::now() + BACKOFF_BASE;
            let backoff = peer_map.insert(ma.to_string(), BackoffAddr { tries: 1, until });
            log::trace!("[DialBackoff] update backoff,{:?}", backoff);
        }
    }

    /// cleanup backoff
    pub fn cleanup(&self) {
        let me = self.clone();
        task::spawn(async move {
            log::trace!("[DialBackoff] waiting cleanup backoff.");
            if let Some(max_time) = me.max_time {
                Delay::new(max_time).await;
            } else {
                Delay::new(BACKOFF_MAX).await;
            }
            log::trace!("[DialBackoff] begin cleanup backoff");
            me.do_cleanup().await;
        });
    }

    async fn do_cleanup(self) {
        let clean_peer_ids = {
            let mut clean_peer_ids = Vec::<PeerId>::new();
            let lock = self.entries.lock().await;
            for (p, e) in lock.iter() {
                let mut good = false;
                let now = Instant::now();
                for backoff in e.values() {
                    let mut backoff_time = BACKOFF_BASE + BACKOFF_COEF * (backoff.tries * backoff.tries);
                    if backoff_time > BACKOFF_MAX {
                        backoff_time = BACKOFF_MAX
                    }
                    log::trace!(
                        "[DialBackoff]  now={:?},backoff.until + backoff_time={:?}",
                        now,
                        (backoff.until + backoff_time)
                    );
                    if now < backoff.until + backoff_time {
                        good = true;
                        break;
                    }
                }
                if !good {
                    clean_peer_ids.push(p.clone());
                }
            }
            clean_peer_ids
        };
        let mut lock = self.entries.lock().await;
        for id in clean_peer_ids {
            let _ = lock.remove(&id);
        }
    }
}

#[derive(Clone)]
pub struct AsyncDial {
    dial_attempts: u32,
}
#[derive(Clone)]
pub(crate) struct DialParam {
    pub(crate) transports: FnvHashMap<u32, ITransportEx>,
    pub(crate) peers: PeerStore,
    pub(crate) event_sender: mpsc::UnboundedSender<SwarmEvent>,
    pub(crate) peer_id: PeerId,
    pub(crate) dial_backoff: DialBackoff,
    pub(crate) dial_limmiter: DialLimiter,
}
impl Default for AsyncDial {
    fn default() -> Self {
        AsyncDial::new()
    }
}
impl AsyncDial {
    pub(crate) fn new() -> Self {
        let mut dial_attempts = DIAL_ATTEMPTS;
        for (key, value) in std::env::vars() {
            if key == "LIBP2P_SWARM_DIAL_ATTEMPTS" {
                dial_attempts = value.parse::<u32>().unwrap();
            }
        }
        Self { dial_attempts }
    }

    pub(crate) fn dial<F>(&self, param: DialParam, f: F) -> Result<()>
    where
        F: FnOnce(Result<Connection>) + Send + 'static,
    {
        let me = self.clone();
        task::spawn(async move {
            let r = me.start(param).await;
            f(r);
        });
        Ok(())
    }

    #[allow(unused_assignments)]
    async fn start(&self, dial_param: DialParam) -> Result<Connection> {
        let mut dial_count: u32 = 0;
        let mut send_r = Err(SwarmError::Internal);
        loop {
            if dial_count > self.dial_attempts {
                send_r = Err(SwarmError::MaxDialAttempts(dial_count - 1));
                break;
            }
            dial_count += 1;

            let active_param = dial_param.clone();
            send_r = self.dial_addrs(active_param).await;
            if send_r.is_err() {
                if dial_count <= self.dial_attempts {
                    log::info!("[AsyncDial] All addresses of this peer cannot be dialed successfully. Now try dialing again. [peer]={:?},[Number of attempts]={}",&dial_param.peer_id,dial_count);
                    //TODO:
                    task::sleep(BACKOFF_BASE).await;
                } else if dial_count > 0 && self.dial_attempts == 0 {
                    break;
                }
                continue;
            } else {
                break;
            }
        }
        send_r
    }

    /// Starts a dialing task
    async fn dial_addrs(&self, param: DialParam) -> Result<Connection> {
        let peer_id = param.peer_id.clone();
        log::debug!("[AsyncDial] dialer, looking for {:?}", &peer_id);
        // check addrs
        let addrs = param.peers.addrs.get_addr(&peer_id);
        if addrs.is_none() {
            return Err(SwarmError::NoAddresses(peer_id));
        }

        // get all addresses from peerstore
        let mut addrs_origin = Vec::<Multiaddr>::new();
        let addrs_clone = addrs.unwrap().clone();
        for i in 0..addrs_clone.len() {
            addrs_origin.push(addrs_clone.get(i).unwrap().addr.clone());
        }
        // TODO: filter Known Undialables address ,If there is no address  can dial return SwarmError::NoGoodAddresses

        // Check backoff
        let mut non_backoff = false;
        for addr in addrs_origin.clone() {
            // skip addresses in back-off
            if !param.dial_backoff.is_backoff(param.peer_id.clone(), addr).await {
                non_backoff = true
            }
        }
        if !non_backoff {
            return Err(SwarmError::DialBackoff);
        }

        // ranks addresses
        let addrs_rank = self.rank_addrs(addrs_origin);

        //dial
        let (tx, mut rx) = mpsc::unbounded::<Result<Box<dyn StreamMuxerEx>>>();
        let n = addrs_rank.len() as u32;
        let is_cancel = Arc::new(AtomicBool::new(false));
        let mut err_count = 0u32;
        let mut conn_result = None;
        for addr in addrs_rank {
            let transport = self.get_transport_for_dialing(param.transports.clone(), addr.clone());
            let mut tx_clone = tx.clone();
            if let Err(e) = transport {
                let _ = tx_clone.send(Err(e)).await;
                continue;
            }
            let dj = DialJob {
                addr,
                peer: peer_id.clone(),
                reply: tx_clone,
                transport: transport.unwrap(),
                is_cancel: is_cancel.clone(),
            };
            // spawn a task to dial
            let dial_limmiter = param.dial_limmiter.clone();
            task::spawn(async move {
                dial_limmiter.add_dial_job(dj).await;
            });
        }
        let me = self.clone();
        for i in 0..n {
            log::trace!("[AsyncDial] receive next dial result,count={}", i);
            let r = rx.next().await;
            match r {
                Some(Ok(stream_muxer)) => {
                    let remote_addr = stream_muxer.remote_multiaddr();
                    let conn_r = me
                        .clone()
                        .handle_stream_muxer(param.dial_backoff.clone(), param.event_sender.clone(), stream_muxer)
                        .await;
                    if let Ok(conn) = conn_r {
                        log::info!("[AsyncDial] Dial success ,[{:?}]", conn);
                        conn_result = Some(conn);
                        //Send cancel dialing signal
                        is_cancel.clone().store(true, Ordering::SeqCst);
                        break;
                    } else {
                        param.dial_backoff.add_backoff(peer_id.clone(), remote_addr.clone()).await;
                        let _ = param
                            .event_sender
                            .clone()
                            .send(SwarmEvent::OutgoingConnectionError {
                                peer_id: peer_id.clone(),
                                remote_addr,
                                error: TransportError::Internal,
                            })
                            .await;
                        err_count += 1;
                        log::info!("[AsyncDial] Dial error: {}", conn_r.unwrap_err().to_string());
                    }
                }
                Some(Err(err)) => {
                    log::info!("[AsyncDial] Dial error: {}", err.to_string());
                    if let SwarmError::TransportDialFailed(ma, e) = err {
                        param.dial_backoff.add_backoff(peer_id.clone(), ma.clone()).await;
                        let _ = param
                            .event_sender
                            .clone()
                            .send(SwarmEvent::OutgoingConnectionError {
                                peer_id: peer_id.clone(),
                                remote_addr: ma,
                                error: e,
                            })
                            .await;
                    }
                    err_count += 1;
                }
                None => {
                    log::info!("[AsyncDial] should not happen");
                }
            }
        }
        if err_count == n {
            return Err(SwarmError::AllDialsFailed);
        } else if conn_result.is_some() {
            return Ok(conn_result.unwrap());
        }
        log::info!("[AsyncDial] dial failed,should not happen");
        Err(SwarmError::Internal)
    }

    async fn handle_stream_muxer(
        &self,
        dial_backoff: DialBackoff,
        mut tx: mpsc::UnboundedSender<SwarmEvent>,
        stream_muxer: Box<dyn StreamMuxerEx>,
    ) -> Result<Connection> {
        let addr: Multiaddr = stream_muxer.remote_multiaddr();
        let peer_id: PeerId = stream_muxer.remote_peer();
        // test if the PeerId matches expectation, otherwise,
        // it is a bad outgoing connection
        if peer_id == stream_muxer.remote_peer() {
            let (conn_tx, conn_rx) = oneshot::channel::<Connection>();
            let _ = tx
                .send(SwarmEvent::ConnectionEstablished {
                    stream_muxer,
                    direction: Direction::Outbound,
                    conn_tx: Some(conn_tx),
                })
                .await;
            // now wait for the connection
            if let Ok(conn) = conn_rx.await {
                Ok(conn)
            } else {
                log::info!("[AsyncDial] should not happen");
                Err(SwarmError::Internal)
            }
        } else {
            let wrong_id = stream_muxer.remote_peer();
            log::info!(
                "[AsyncDial] bad connection, peerid mismatch conn={:?} wanted={:?} got={:?}",
                stream_muxer,
                &peer_id,
                wrong_id
            );
            let _ = tx
                .send(SwarmEvent::OutgoingConnectionError {
                    peer_id: peer_id.clone(),
                    remote_addr: addr.clone(),
                    error: TransportError::Internal,
                })
                .await;
            // add backoff
            dial_backoff.add_backoff(peer_id, addr).await;
            Err(SwarmError::InvalidPeerId(wrong_id))
        }
    }
    ///  retrieves the appropriate transport for  dial the given multiaddr.
    fn get_transport_for_dialing(&self, transports: FnvHashMap<u32, ITransportEx>, mut addr: Multiaddr) -> Result<ITransportEx> {
        let protocol = addr.pop();
        match protocol {
            Some(d) => {
                log::debug!("[AsyncDial] get transport for dialing: addr={} protocol={}", &addr, &d);
                let id = d
                    .get_key()
                    .map_err(|_| SwarmError::Transport(TransportError::MultiaddrNotSupported(addr)))?;
                if let Some(selected) = transports.get(&id).map(|s| s.box_clone()) {
                    return Ok(selected);
                }
                Err(SwarmError::TransportsNotSupported(d.to_string()))
            }
            None => Err(SwarmError::Transport(TransportError::MultiaddrNotSupported(addr))),
        }
    }
    /// ranks addresses in descending order of preference for dialing   Private UDP > Public UDP > Private TCP > Public TCP > UDP Relay server > TCP Relay server
    fn rank_addrs(&self, addrs: Vec<Multiaddr>) -> Vec<Multiaddr> {
        let mut local_udp_addrs = Vec::<Multiaddr>::new(); // private udp
        let mut relay_udp_addrs = Vec::<Multiaddr>::new(); // relay udp
        let mut others_udp = Vec::<Multiaddr>::new(); // public udp
        let mut local_fd_addrs = Vec::<Multiaddr>::new(); // private fd consuming
        let mut relay_fd_addrs = Vec::<Multiaddr>::new(); //  relay fd consuming
        let mut others_fd = Vec::<Multiaddr>::new(); // public fd consuming
        let mut relays = Vec::<Multiaddr>::new();
        let mut fds = Vec::<Multiaddr>::new();
        let mut rank = Vec::<Multiaddr>::new();
        for addr in addrs.into_iter() {
            if addr.clone().value_for_protocol(protocol::P2P_CIRCUIT).is_some() {
                if addr.should_consume_fd() {
                    relay_fd_addrs.push(addr);
                    continue;
                }
                relay_udp_addrs.push(addr);
            } else if addr.clone().is_private_addr() {
                if addr.should_consume_fd() {
                    local_fd_addrs.push(addr);
                    continue;
                }
                local_udp_addrs.push(addr);
            } else {
                if addr.should_consume_fd() {
                    others_fd.push(addr);
                    continue;
                }
                others_udp.push(addr);
            }
        }
        relays.append(&mut relay_udp_addrs);
        relays.append(&mut relay_fd_addrs);
        fds.append(&mut local_fd_addrs);
        fds.append(&mut others_fd);
        rank.append(&mut local_udp_addrs);
        rank.append(&mut others_udp);
        rank.append(&mut fds);
        rank.append(&mut relays);
        rank
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use libp2prs_core::{Multiaddr, PeerId};
    use std::str::FromStr;
    use std::time::Duration;

    #[test]
    fn test_dial_is_backoff() {
        let ab = DialBackoff::default();
        let peer_id = PeerId::from_str("12D3KooWDpJ7As7BWAwRMfu1VU2WCqNjvq387JEYKDBj4kx6nXTN").unwrap();
        let dial_addr = Multiaddr::from_str("/ip4/127.0.0.1/tcp/8086").unwrap();
        let r = task::block_on(async {
            ab.add_backoff(peer_id.clone(), dial_addr.clone()).await;
            ab.is_backoff(peer_id, dial_addr).await
        });
        assert_eq!(r, true);
    }

    #[test]
    fn test_dial_isnot_backoff() {
        let ab = DialBackoff::default();
        let peer_id = PeerId::from_str("12D3KooWDpJ7As7BWAwRMfu1VU2WCqNjvq387JEYKDBj4kx6nXTN").unwrap();
        let dial_addr = Multiaddr::from_str("/ip4/127.0.0.1/tcp/8086").unwrap();
        let r = task::block_on(async {
            ab.add_backoff(peer_id.clone(), dial_addr.clone()).await;
            let backoff_time = {
                let lock = ab.entries.lock().await;
                let backoff_addr = lock.get(&peer_id).unwrap().get(&dial_addr.to_string()).unwrap();
                BACKOFF_BASE + BACKOFF_COEF * (backoff_addr.tries * backoff_addr.tries)
            };
            task::sleep(backoff_time).await;
            ab.is_backoff(peer_id.clone(), dial_addr.clone()).await
        });
        assert_eq!(r, false);
    }

    #[test]
    fn test_dial_backoff_cleanup() {
        let ab = DialBackoff::default().with_max_time(Duration::from_secs(12));
        ab.cleanup();
        let peer_id = PeerId::from_str("12D3KooWDpJ7As7BWAwRMfu1VU2WCqNjvq387JEYKDBj4kx6nXTN").unwrap();
        let dial_addr = Multiaddr::from_str("/ip4/127.0.0.1/tcp/8086").unwrap();
        let ab_clone = ab.clone();
        let r1 = task::block_on(async {
            ab_clone.add_backoff(peer_id.clone(), dial_addr.clone()).await;
            ab_clone.is_backoff(peer_id.clone(), dial_addr.clone()).await
        });
        let r2 = task::block_on(async {
            task::sleep(Duration::from_secs(13)).await;
            ab.entries.lock().await.get(&peer_id).is_none()
        });
        assert_eq!(r1, true);
        assert_eq!(r2, true);
    }
}
