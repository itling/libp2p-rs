use crate::connection::{Connection, Direction};
use crate::{Swarm, SwarmControlCmd, SwarmError, SwarmEvent};
use async_std::sync::Arc;
use async_std::{future, task};
use fnv::FnvHashMap;
use futures::channel::{mpsc, oneshot};
use futures::lock::Mutex;
use futures::prelude::*;
use futures_timer::Delay;
use libp2prs_core::peerstore::PeerStore;
use libp2prs_core::transport::upgrade::ITransportEx;
use libp2prs_core::transport::TransportError;
use libp2prs_core::{
    multiaddr::{protocol, Multiaddr},
    PeerId,
};
use std::sync::atomic::Ordering;
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicU32};
use std::time::{Duration, Instant};

type Result<T> = std::result::Result<T, SwarmError>;

/// CONCURRENT_FD_DIALS is the number of concurrent outbound dials over transports that consume file descriptors
const CONCURRENT_FD_DIALS: u32 = 160;

/// DEFAULT_PER_PEER_RATE_LIMIT is the number of concurrent outbound dials to makeper peer
const DEFAULT_PER_PEER_RATE_LIMIT: u32 = 8;

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
    event_sender: mpsc::UnboundedSender<SwarmEvent>,
    reply: mpsc::UnboundedSender<Result<Connection>>,
    transport: ITransportEx,
    is_cancel: Arc<AtomicBool>,
    dial_backoff: DialBackoff,
}
#[derive(Clone)]
pub(crate) struct DialLimiter {
    fd_consuming: Arc<AtomicU32>,
    fd_limit: u32,
    per_peer_limit: u32,
    waiting_on_fd: Arc<Mutex<Vec<DialJob>>>,
    active_per_peer: Arc<Mutex<FnvHashMap<PeerId, u32>>>,
    waiting_on_peer_limit: Arc<Mutex<FnvHashMap<PeerId, Vec<DialJob>>>>,
}

impl Default for DialLimiter {
    fn default() -> Self {
        Self::new()
    }
}

impl DialLimiter {
    fn new() -> Self {
        let mut fd_limit = CONCURRENT_FD_DIALS;
        let mut per_peer_limit = DEFAULT_PER_PEER_RATE_LIMIT;
        for (key, value) in std::env::vars() {
            if key == "LIBP2P_SWARM_FD_LIMIT" {
                fd_limit = value.parse::<u32>().unwrap();
            } else if key == "LIBP2P_SWARM_PER_PEER_RATE_LIMIT" {
                per_peer_limit = value.parse::<u32>().unwrap();
            }
        }
        DialLimiter {
            fd_consuming: Arc::new(AtomicU32::new(0)),
            fd_limit,
            per_peer_limit,
            waiting_on_fd: Default::default(),
            active_per_peer: Default::default(),
            waiting_on_peer_limit: Default::default(),
        }
    }

    fn dial_timeout(&self, ma: Multiaddr) -> Duration {
        let mut timeout: Duration = DIAL_TIMEOUT;
        if ma.is_private_addr() {
            timeout = DIAL_TIMEOUT_LOCAL;
        }
        timeout
    }

    /// free_fd_token frees FD token and if there are any schedules another waiting dialJob in it's place
    async fn free_fd_token(&self) {
        let fd_lock = self.waiting_on_fd.lock().await;
        log::debug!(
            "[DialLimiter] freeing FD token; waiting: {}; consuming: {:?}",
            &fd_lock.len(),
            &self.fd_consuming
        );
        self.fd_consuming.fetch_sub(1, Ordering::SeqCst);
    }

    /// execute job: waiting on fd
    async fn exec_waiting_on_fd_job(&self) {
        let jobs = {
            let mut guard_waiting = self.waiting_on_fd.lock().await;
            let n = guard_waiting.len();
            let mut jobs = Vec::<DialJob>::with_capacity(n);
            for i in 0..n {
                let dj = guard_waiting.remove(i);
                jobs.push(dj);
                self.fd_consuming.fetch_add(1_u32, Ordering::SeqCst);
            }
            jobs
        };
        for dj in jobs {
            self.execute_dial(dj).await;
        }
    }

    async fn free_peer_token(&self, dj: DialJob) {
        let mut guard_active = self.active_per_peer.lock().await;
        let mut guard_waiting = self.waiting_on_peer_limit.lock().await;
        let limit = guard_waiting.entry(dj.clone().peer).or_insert_with(Default::default);
        if let Some(active_per_peer) = guard_active.get_mut(&dj.peer) {
            log::debug!(
                "[DialLimiter] freeing peer token; peer {}; addr: {}; active for peer: {}; waiting on peer limit: {}",
                &dj.peer.clone(),
                &dj.addr,
                active_per_peer,
                limit.len()
            );
            if *active_per_peer > 0 {
                *active_per_peer -= 1;
            }
        }
    }

    /// execute job: waiting on peerlimit
    async fn exec_waiting_on_peer_limit_job(&self, dj: DialJob) {
        let jobs = {
            let mut jobs = Vec::<DialJob>::new();
            let mut guard_active = self.active_per_peer.lock().await;
            let active_per_peer = guard_active.get(&dj.peer).unwrap().to_owned();
            let peer = dj.clone().peer;
            let mut guard_waiting = self.waiting_on_peer_limit.lock().await;
            if guard_waiting.get(&peer).is_some() {
                let waiting_list = guard_waiting.remove(&peer).unwrap(); //TODO unwrap
                for job in waiting_list.into_iter() {
                    guard_active.insert(job.clone().peer, active_per_peer + 1);
                    jobs.push(job);
                }
            }
            jobs
        };
        for job in jobs {
            self.add_check_fd_limit(job).await;
        }
    }

    async fn clear_all_peer_dials(&self, peer: PeerId) {
        let mut guard_waiting = self.waiting_on_peer_limit.lock().await;
        if guard_waiting.get(&peer).is_some() {
            let _ = guard_waiting.remove(&peer);
        }
        log::debug!("[DialLimiter] clearing all peer dials: {}", &peer)
    }

    async fn exec_waiting_job(&self, dj: DialJob) {
        log::debug!("[DialLimiter] exec all waiting job");
        if self.should_consume_fd(dj.addr.clone()) {
            self.exec_waiting_on_peer_limit_job(dj).await;
        }
        self.exec_waiting_on_fd_job().await;
    }

    fn should_consume_fd(&self, addr: Multiaddr) -> bool {
        should_consume_fd(addr)
    }

    async fn finished_dial(&self, dj: DialJob) {
        if self.should_consume_fd(dj.addr.clone()) {
            self.free_fd_token().await;
        }
        self.free_peer_token(dj).await;
    }

    async fn add_check_fd_limit(&self, dj: DialJob) {
        //for unlock
        {
            let mut lock = self.waiting_on_fd.lock().await;
            if self.should_consume_fd(dj.addr.clone()) {
                if self.fd_consuming.load(Ordering::SeqCst) >= self.fd_limit {
                    log::trace!(
                        "[DialLimiter] blocked dial waiting on FD token; peer: {}; addr: {}; consuming: {:?}; limit: {:?}; waiting: {}",
                        &dj.peer,
                        &dj.addr,
                        self.fd_consuming,
                        self.fd_limit,
                        lock.len()
                    );
                    lock.push(dj);
                    return;
                }
                log::debug!(
                    "[DialLimiter] taking FD token: peer: {}; addr: {}; prev consuming: {:?}",
                    dj.peer,
                    dj.addr,
                    self.fd_consuming
                );
                // take token
                self.fd_consuming.fetch_add(1, Ordering::SeqCst);
            }

            log::debug!(
                "[DialLimiter] executing dial; peer: {}; addr: {}; FD consuming: {:?}; waiting: {}",
                dj.peer,
                dj.addr,
                self.fd_consuming,
                lock.len()
            );
        }
        let djc = dj.clone();
        let _ = self.execute_dial(djc).await;
    }

    async fn add_check_peer_limit(&self, dj: DialJob) {
        //for unlock
        let active_per_peer: u32 = {
            let active_cc = self.active_per_peer.clone();
            let mut guard_active = active_cc.lock().await;
            let active_per_peer = guard_active.get(&dj.peer).unwrap_or_else(|| &0u32).to_owned();
            let mut guard_waiting = self.waiting_on_peer_limit.lock().await;
            let limit = guard_waiting.entry(dj.clone().peer).or_insert_with(Default::default);
            if active_per_peer >= self.per_peer_limit {
                log::trace!(
                    "[DialLimiter] blocked dial waiting on peer limit; peer: {}; addr: {}; active: {}; 
                peer limit: {:?}; waiting: {}",
                    &dj.peer,
                    &dj.addr,
                    active_per_peer,
                    &self.per_peer_limit,
                    limit.len()
                );
                limit.push(dj.clone());
            } else {
                guard_active.insert(dj.peer.clone(), active_per_peer + 1);
            }
            active_per_peer
        };
        if active_per_peer < self.per_peer_limit {
            self.add_check_fd_limit(dj).await
        }
    }

    /// add_dial_job tries to take the needed tokens for starting the given dial job.
    /// If it acquires all needed tokens, it immediately starts the dial, otherwise
    /// it will put it on the waitlist for the requested token.
    async fn add_dial_job(&self, dj: DialJob) {
        log::debug!("[DialLimiter] adding a dial job through limiter: {}", dj.addr);
        self.add_check_peer_limit(dj).await;
    }

    /// execute_dial calls the dial_handler trait object, and reports the result through the response
    /// channel when finished. Once the response is sent it also releases all tokens
    /// it held during the dial.
    async fn execute_dial(&self, job: DialJob) {
        let djc_dial = job.clone();
        let mut djc_reply = job.clone();
        let djc_finish = job.clone();
        let self_cc = self.clone();
        let timeout = self.dial_timeout(job.addr.clone());
        let dial_r = future::timeout(timeout, self_cc.do_dial(djc_dial)).await;
        if dial_r.is_ok() {
            let _ = djc_reply.reply.send(dial_r.unwrap()).await;
        } else {
            let _ = djc_reply
                .reply
                .send(Err(SwarmError::DialTimeout(job.addr, timeout.as_secs())))
                .await;
        }
        self.finished_dial(djc_finish).await;
    }

    /// Tries to dial the given address.
    ///
    /// Returns an error if the address is not supported.
    async fn do_dial(&self, job: DialJob) -> Result<Connection> {
        let addr = job.addr.clone();
        let peer_id = job.peer.clone();
        log::info!("[DialLimiter] dialing addr={:?}, expecting {:?}", &addr, &peer_id);
        if job.is_cancel.load(Ordering::SeqCst) {
            log::info!(
                "[DialLimiter] Cancel the current task because another task has already succeeded, addr={}",
                &addr
            );
            return Err(SwarmError::DialCancelled(addr.clone()));
        }
        //mock
        //task::sleep(Duration::from_secs(1)).await;
        let mut tx = job.clone().event_sender;
        let mut transport = job.clone().transport;
        let r = transport.dial(addr.clone()).await;
        let response = match r {
            Ok(stream_muxer) => {
                //double check
                if job.is_cancel.load(Ordering::SeqCst) {
                    //close stream
                    let _ = stream_muxer.to_owned().as_mut().close();
                    log::info!(
                        "[DialLimiter] Cancel the current task because another task has already succeeded, addr={}",
                        &addr
                    );
                    return Err(SwarmError::DialCancelled(addr.clone()));
                }
                // test if the PeerId matches expectation, otherwise,
                // it is a bad outgoing connection
                if job.peer == stream_muxer.remote_peer() {
                    let (conn_tx, conn_rx) = oneshot::channel::<Connection>();
                    let _ = tx
                        .send(SwarmEvent::ConnectionEstablished {
                            stream_muxer,
                            direction: Direction::Outbound,
                            conn_tx,
                        })
                        .await;
                    let conn = conn_rx.await;
                    Ok(conn.unwrap())
                } else {
                    let wrong_id = stream_muxer.remote_peer();
                    log::info!(
                        "[DialLimiter] bad connection, peerid mismatch conn={:?} wanted={:?} got={:?}",
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
                    let jobc = job.clone();
                    jobc.dial_backoff.add_backoff(peer_id.clone(), addr.clone()).await;
                    Err(SwarmError::InvalidPeerId(wrong_id))
                }
            }
            Err(err) => {
                let _ = tx
                    .send(SwarmEvent::OutgoingConnectionError {
                        peer_id: peer_id.clone(),
                        remote_addr: addr.clone(),
                        error: TransportError::Internal,
                    })
                    .await;
                // add backoff
                let jobc = job.clone();
                jobc.dial_backoff.add_backoff(peer_id.clone(), addr.clone()).await;
                Err(SwarmError::Transport(err))
            }
        };
        response
    }
}

/// we don't consume FD's for relay addresses for now as they will be consumed when the Relay Transport actually dials the Relay server. That dial call will also pass through this limiter with the address of the relay server i.e. non-relay address.
fn should_consume_fd(addr: Multiaddr) -> bool {
    let mut find = false;
    for p in addr.iter() {
        if p.get_key().is_ok() && (p.get_key().unwrap() == protocol::UNIX || p.get_key().unwrap() == protocol::TCP) {
            find = true;
        }
    }
    find
}

/// ranks addresses in descending order of preference for dialing   Private UDP > Public UDP > Private TCP > Public TCP > UDP Relay server > TCP Relay server
pub fn rank_addrs(addrs: Vec<Multiaddr>) -> Vec<Multiaddr> {
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
            if should_consume_fd(addr.clone()) {
                relay_fd_addrs.push(addr);
                continue;
            }
            relay_udp_addrs.push(addr);
        } else if addr.clone().is_private_addr() {
            if should_consume_fd(addr.clone()) {
                local_fd_addrs.push(addr);
                continue;
            }
            local_udp_addrs.push(addr);
        } else {
            if should_consume_fd(addr.clone()) {
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

// DialBackoff is a type for tracking peer dial backoffs.
//
// * It's safe to use its zero value.
// * It's thread-safe.
// * It's *not* safe to move this type after using.
#[derive(Clone)]
pub(crate) struct DialBackoff {
    entries: Arc<Mutex<FnvHashMap<PeerId, FnvHashMap<String, BackoffAddr>>>>,
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

impl DialBackoff {
    pub fn new() -> Self {
        Self {
            entries: Default::default(),
        }
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
    ///     BackoffBase + BakoffCoef * PriorBackoffs^2
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

    /// BACKOFF_MAX
    pub fn cleanup(self) {
        task::spawn(async move {
            log::trace!("[DialBackoff] waiting cleanup backoff.");
            Delay::new(BACKOFF_MAX).await;
            log::trace!("[DialBackoff] begin cleanup backoff");
            let mut peer_ids = Vec::<PeerId>::new();
            {
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
                            backoff.until + backoff_time
                        );
                        if now < backoff.until + backoff_time {
                            good = true;
                            break;
                        }
                    }
                    if !good {
                        peer_ids.push(p.clone());
                    }
                }
            }
            let mut lock = self.entries.lock().await;
            for id in peer_ids {
                let _ = lock.remove(&id);
            }
        });
    }
}

/// DialSync is a dial synchronization helper that ensures that at most one dial to any given peer is active at any given time.
#[derive(Clone)]
pub(crate) struct DialSync {
    dials: Arc<Mutex<FnvHashMap<PeerId, ActiveDial>>>,
}
#[derive(Clone)]
struct ActiveDial {
    id: PeerId,
    ref_cnt: Arc<AtomicI32>,
    ds: DialSync,
    dial_attempts: u32,
}
#[derive(Clone)]
pub(crate) struct ActiveDialParam {
    pub(crate) transports: FnvHashMap<u32, ITransportEx>,
    pub(crate) peers: PeerStore,
    pub(crate) event_sender: mpsc::UnboundedSender<SwarmEvent>,
    pub(crate) ctrl_sender: mpsc::Sender<SwarmControlCmd>,
    pub(crate) peer_id: PeerId,
    pub(crate) dial_backoff: DialBackoff,
    pub(crate) dial_limmiter: DialLimiter,
}

impl ActiveDial {
    fn new(id: PeerId, ds: DialSync) -> Self {
        let mut dial_attempts = DIAL_ATTEMPTS;
        for (key, value) in std::env::vars() {
            if key == "LIBP2P_SWARM_DIAL_ATTEMPTS" {
                dial_attempts = value.parse::<u32>().unwrap();
            }
        }
        Self {
            id,
            ref_cnt: Default::default(),
            ds,
            dial_attempts,
        }
    }

    fn incref(&self) {
        self.ref_cnt.fetch_add(1_i32, Ordering::SeqCst);
    }

    async fn decref(&self) {
        self.ref_cnt.fetch_sub(1_i32, Ordering::SeqCst);
        let maybe_zero = self.ref_cnt.load(Ordering::SeqCst) <= 0;
        if maybe_zero && self.ref_cnt.load(Ordering::SeqCst) <= 0 {
            self.ds.dials.lock().await.remove(&self.id);
        }
    }

    #[allow(unused_assignments)]
    async fn start(self, wait_tx: oneshot::Sender<Result<Connection>>, param: ActiveDialParam) {
        let mut dial_count: u32 = 0;
        let mut send_r = Err(SwarmError::Internal);
        let clone = param.clone();
        loop {
            if dial_count > self.dial_attempts {
                send_r = Err(SwarmError::MaxDialAttempts(dial_count - 1));
                break;
            }
            dial_count += 1;

            let active_param_cc = clone.clone();
            send_r = self.dail(active_param_cc).await;
            if send_r.is_err() {
                if dial_count <= self.dial_attempts {
                    log::error!("[DialSync] All addresses of this peer cannot be dialed successfully. Now try dialing again. [peer]={:?},[Number of attempts]={}",clone.peer_id,dial_count);
                    //TODO: sleep time?
                    task::sleep(BACKOFF_BASE).await;
                } else if dial_count > 0 && self.dial_attempts == 0 {
                    break;
                }
                continue;
            } else {
                break;
            }
        }
        let _ = wait_tx.send(send_r);
    }

    pub async fn wait(&mut self, wait_rx: oneshot::Receiver<Result<Connection>>) -> Result<Connection> {
        if let Ok(e) = wait_rx.await {
            self.decref().await;
            e
        } else {
            log::error!("[DialSync] should not happen");
            self.decref().await;
            Err(SwarmError::Internal)
        }
    }

    /// Starts a dialing task
    pub async fn dail(&self, param: ActiveDialParam) -> Result<Connection> {
        log::info!("[DialSync] dialer, looking for {:?}", param.peer_id);
        let addrs = param.peers.addrs.get_addr(&param.peer_id);
        if addrs.is_none() {
            return Err(SwarmError::NoAddresses(param.peer_id));
        }
        let mut addrs_origin = Vec::<Multiaddr>::new();
        let addrs_clone = addrs.unwrap().clone();
        for i in 0..addrs_clone.len() {
            addrs_origin.push(addrs_clone.get(i).unwrap().addr.clone());
        }
        // TODO: filter Known Undialables address ,return SwarmError::NoGoodAddresses

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
        let addrs_rank = rank_addrs(addrs_origin);
        //dial
        let (tx, mut rx) = mpsc::unbounded::<Result<Connection>>();
        let n = addrs_rank.len() as u32;
        let is_cancel = Arc::new(AtomicBool::new(false));
        let mut ctrl_sender = param.ctrl_sender.clone();
        for addr in addrs_rank {
            let transport = Swarm::get_best_transport(param.transports.clone(), addr.clone());
            if transport.is_err() {
                return Err(SwarmError::NoTransport);
            }
            let dj = DialJob {
                addr,
                peer: param.peer_id.clone(),
                event_sender: param.event_sender.clone(),
                reply: tx.clone(),
                transport: transport.unwrap(),
                is_cancel: is_cancel.clone(),
                dial_backoff: param.dial_backoff.clone(),
            };
            let dial_limmiter = param.dial_limmiter.clone();
            let djc = dj.clone();
            task::spawn(async move {
                dial_limmiter.add_dial_job(djc).await;
                //task::sleep(Duration::from_secs(1)).await;
                dial_limmiter.exec_waiting_job(dj).await;
            });
        }
        let mut count_err = 0u32;
        let mut dial_result = false;
        let mut conn_option = None;
        for _i in 0..n {
            log::trace!("[DialSync] rev next dial result,count={}", _i);
            let r = rx.next().await;
            match r {
                Some(Ok(conn)) => {
                    log::info!("[DialSync] Dial success ,[{:?}]", conn);
                    dial_result = true;
                    conn_option = Some(conn);
                    //Send cancel dialing signal
                    is_cancel.clone().compare_and_swap(false, true, Ordering::SeqCst);
                    break;
                }
                Some(Err(e)) => {
                    count_err += 1;
                    log::error!("[DialSync] Dial error: {:?}", e);
                    continue;
                }
                None => {
                    log::error!("[DialSync] should not happen");
                }
            }
        }
        let dial_limmiter = param.dial_limmiter.clone();
        dial_limmiter.clear_all_peer_dials(param.peer_id.clone()).await;
        if count_err == n {
            Err(SwarmError::AllDialsFailed)
        } else if dial_result {
            if let Some(conn) = conn_option {
                //Maybe connection has been added to swarm，so it needs to be cleaned up
                let id = conn.id();
                let _ = ctrl_sender
                    .send(SwarmControlCmd::CleanupConnection(param.peer_id.clone(), id))
                    .await;
                Ok(conn)
            } else {
                log::trace!("[DialSync] return SwarmError::NoConnection");
                Err(SwarmError::NoConnection(param.peer_id))
            }
        } else {
            log::trace!("[DialSync] return SwarmError::NoConnection");
            Err(SwarmError::NoConnection(param.peer_id))
        }
    }
}
impl Default for DialSync {
    fn default() -> Self {
        Self::new()
    }
}
impl DialSync {
    pub(crate) fn new() -> Self {
        Self { dials: Default::default() }
    }

    pub(crate) fn dial_lock(self, param: ActiveDialParam, reply: oneshot::Sender<Result<Connection>>) -> Result<()> {
        let (wait_tx, wait_rx) = oneshot::channel();
        let id = param.peer_id.clone();
        let actd = ActiveDial::new(id.clone(), self.clone());
        let actd_clone = actd.clone();
        task::spawn(async move {
            let dials = self.dials.clone();
            let mut lock = dials.lock().await;
            if lock.get(&id).is_none() {
                let v = lock.insert(id.clone(), actd_clone.clone());
                if v.is_none() {
                    let _ = actd_clone.clone().start(wait_tx, param).await;
                }
            }
        });
        actd.incref();
        task::spawn(async move {
            let r = actd.clone().wait(wait_rx).await;
            let _ = reply.send(r);
        });
        Ok(())
    }
}

#[test]
fn test_basic_dial_sync() {}
