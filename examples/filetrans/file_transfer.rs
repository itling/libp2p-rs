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

use async_std::task;
use async_trait::async_trait;

#[macro_use]
extern crate lazy_static;

use async_std::fs::{self, *};
use futures::channel::{mpsc, oneshot};
use futures_util::io::AsyncWriteExt;
use libp2prs_core::identity::Keypair;
use libp2prs_core::transport::upgrade::TransportUpgrade;
use libp2prs_core::upgrade::{Selector, UpgradeInfo};
use std::error::Error;
use std::sync::Arc;

use futures::future::{select, Either};
use futures::lock::Mutex;
use futures::prelude::*;
use futures_timer::Delay;
use libp2prs_core::{Multiaddr, PeerId};
use libp2prs_mplex as mplex;
use libp2prs_plaintext as plaintext;
use libp2prs_secio as secio;
use libp2prs_swarm::protocol_handler::{IProtocolHandler, Notifiee, ProtocolHandler};
use libp2prs_swarm::substream::Substream;
use libp2prs_swarm::Swarm;
use libp2prs_tcp::TcpConfig;
use libp2prs_traits::{ReadEx, WriteEx};
use libp2prs_websocket::WsConfig;
use libp2prs_yamux as yamux;
use rand::distributions::Alphanumeric;
use rand::Rng;
use std::str::FromStr;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};
use structopt::StructOpt;

#[derive(StructOpt)]
struct Config {
    client_or_server: String,
}

fn main() {
    env_logger::from_env(env_logger::Env::default().default_filter_or("info")).init();
    let options = Config::from_args();
    if options.client_or_server == "server" {
        log::info!("Starting server ......");
        run_server(38889);
    } else if options.client_or_server == "client" {
        log::info!("Starting client ......");
        test_file_transfer_with_mplex(38889);
    } else {
        panic!("param error")
    }
}

fn test_file_transfer_with_mplex(port: u32) {
    //1m 10m 100m 1g
    let loop_v = vec![1000];
    let mut handles = vec![];
    for v in loop_v.into_iter() {
        let handle = task::spawn(async move {
            run_client(v, 3, MuxType::Mplex, TransType::Tcp, port);
        });
        handles.push(handle);
        // let handle = task::spawn(async move {
        //     run_client(v, 3, MuxType::Mplex, TransType::Websocket, port);
        // });
        // handles.push(handle);
    }

    task::block_on(async {
        for handle in handles {
            handle.await;
        }
    });
}

fn test_file_transfer_with_yamux(port: u32) {
    //1m 10m 100m 1g
    let loop_v = vec![1000];
    let mut handles = vec![];
    for v in loop_v.into_iter() {
        // let handle = task::spawn(async move {
        //     run_client(v, 1, MuxType::Yamux, TransType::Tcp, port);
        // });
        // handles.push(handle);

        let handle = task::spawn(async move {
            run_client(v, 1, MuxType::Yamux, TransType::Websocket, port);
        });
        handles.push(handle);
    }

    task::block_on(async {
        for handle in handles {
            handle.await;
        }
    });
}

lazy_static! {
    static ref SERVER_KEY: Keypair = Keypair::generate_ed25519_fixed();
}

#[derive(Clone)]
struct FileTransHandler;

impl UpgradeInfo for FileTransHandler {
    type Info = &'static [u8];

    fn protocol_info(&self) -> Vec<Self::Info> {
        vec![b"/filetrans/1.0.0"]
    }
}

impl Notifiee for FileTransHandler {}

#[async_trait]
impl ProtocolHandler for FileTransHandler {
    async fn handle(&mut self, stream: Substream, _info: <Self as UpgradeInfo>::Info) -> Result<(), Box<dyn Error>> {
        log::info!("[server]  FileTransHandler handling inbound {:?}", stream);
        let start = Instant::now();
        let mut buf = [0; 4096];
        let r_total = Arc::new(AtomicUsize::new(0));
        let sc = stream.clone();
        let r_total_c = r_total.clone();
        let rate_handler = task::spawn(async move { print_trans_rate(sc, r_total_c).await });
        while let Ok(n) = stream.read2(&mut buf[..]).await {
            r_total.fetch_add(n, Ordering::SeqCst);
            log::trace!(" [server] read data={}", n);
            s.write_all2(&buf[0..n]).await.unwrap();
        }
        rate_handler.cancel().await;
        let sec = start.elapsed().as_secs() as usize;
        let data = r_total.load(Ordering::SeqCst);
        let rate = (data / 1024) / sec;
        log::info!("stream:{:?} , total bytes={}, avg rate={}kb/s ", stream, data, rate);

        Ok(())
    }

    fn box_clone(&self) -> IProtocolHandler {
        Box::new(self.clone())
    }
}

async fn print_trans_rate<C>(stream: C, r_total: Arc<AtomicUsize>)
where
    C: ReadEx + WriteEx + std::fmt::Debug,
{
    let start = Instant::now();
    let mut last_r_total = 0;
    loop {
        Delay::new(Duration::from_secs(5)).await;
        let sec = start.elapsed().as_secs() as usize;
        let data = r_total.load(Ordering::SeqCst);
        if data == last_r_total {
            log::info!("stream:{:?} , total bytes={}, rate={}kb/s ", stream, data, 0);
        } else {
            let rate = (data / 1024) / sec;
            log::info!("stream:{:?} , total bytes={}, rate={}kb/s ", stream, data, rate);
        }
        last_r_total = data
    }
}

#[allow(clippy::empty_loop)]
fn run_server(port: u32) {
    let keys = SERVER_KEY.clone();
    let listen_addr1 = format!("/ip4/127.0.0.1/tcp/{}", port).parse().unwrap();
    let listen_addr2 = format!("/ip4/127.0.0.1/tcp/{}/ws", port + 1).parse().unwrap();
    let sec = secio::Config::new(keys.clone());
    //let sec= plaintext::PlainTextConfig::new(keys.clone());

    let mut y_config = yamux::Config::new();
    //y_config.set_max_buffer_size(100 * 1024 * 1024).set_receive_window(256*1024).set_max_message_size(64*1024);
    let mux = Selector::new(y_config, mplex::Config::new());
    let tu1 = TransportUpgrade::new(TcpConfig::default(), mux.clone(), sec.clone());
    let tu2 = TransportUpgrade::new(WsConfig::default(), mux, sec);

    let mut swarm = Swarm::new(keys.public())
        .with_transport(Box::new(tu1))
        .with_transport(Box::new(tu2))
        .with_protocol(Box::new(FileTransHandler {}));

    log::info!("Swarm created, local-peer-id={:?}", swarm.local_peer_id());

    let _control = swarm.control();

    swarm.listen_on(vec![listen_addr1, listen_addr2]).unwrap();

    swarm.start();

    loop {}
}

#[derive(Eq, PartialEq)]
enum MuxType {
    Mplex,
    Yamux,
}
#[derive(Eq, PartialEq)]
enum TransType {
    Tcp,
    Websocket,
}

fn run_client(max_length: usize, stream_count: u32, mux_type: MuxType, trans_type: TransType, port: u32) {
    let keys = Keypair::generate_secp256k1();
    let dial_addr1 = Multiaddr::from_str(&format!("/ip4/127.0.0.1/tcp/{}", port)).unwrap();
    //let dial_addr2 = Multiaddr::from_str(&format!("/ip4/127.0.0.1/tcp/{}/ws", port + 1)).unwrap();
    let sec = secio::Config::new(keys.clone());
    //let sec= plaintext::PlainTextConfig::new(keys.clone());
    let mut swarm = Swarm::new(keys.public());
    swarm = {
        if mux_type == MuxType::Mplex {
            if trans_type == TransType::Tcp {
                let tu = TransportUpgrade::new(TcpConfig::default(), mplex::Config::new(), sec.clone());
                swarm.with_transport(Box::new(tu))
            } else if trans_type == TransType::Websocket {
                let tu = TransportUpgrade::new(WsConfig::default(), mplex::Config::new(), sec.clone());
                swarm.with_transport(Box::new(tu))
            } else {
                panic!("unknown trans_type ")
            }
        } else if mux_type == MuxType::Yamux {
            let mut y_config = yamux::Config::new();
            //y_config.set_max_buffer_size(100 * 1024 * 1024).set_receive_window(256*1024).set_max_message_size(64*1024);
            if trans_type == TransType::Tcp {
                let tu = TransportUpgrade::new(TcpConfig::default(), y_config, sec.clone());
                swarm.with_transport(Box::new(tu))
            } else if trans_type == TransType::Websocket {
                let tu = TransportUpgrade::new(WsConfig::default(), y_config, sec.clone());
                swarm.with_transport(Box::new(tu))
            } else {
                panic!("unknown trans_type ")
            }
        } else {
            panic!("unknown mux_type ")
        }
    };

    let mut control = swarm.control();

    let remote_peer_id = PeerId::from_public_key(SERVER_KEY.public());

    log::info!("about to connect to {:?}", remote_peer_id);

    swarm.peer_addrs_add(&remote_peer_id, dial_addr1, Duration::default());
    //swarm.peer_addrs_add(&remote_peer_id, dial_addr2, Duration::default());

    swarm.start();

    task::block_on(async move {
        control.new_connection(remote_peer_id.clone()).await.unwrap();
        let mut handles = vec![];
        let start = Instant::now();
        for i in 0..stream_count {
            let path = generate_rand_file(max_length).await;
            let mut stream = control.new_stream(remote_peer_id.clone(), vec![b"/filetrans/1.0.0"]).await.unwrap();
            let handle = task::spawn(async move {
                let s1 = stream.clone();

                let file = File::open(&path).await.unwrap();
                let file_length = file.metadata().await.unwrap().len() as usize;
                log::info!("stream opened, read file [{}],length={}", &path, file_length);
                let w_handler = task::spawn(async { read_file_and_write_stream(s1, file).await });

                let s2 = stream.clone();
                let rev_file_name = format!("{}-rev-{}", &path, i);
                let rev_file_name_c = rev_file_name.clone();
                let r_handler = task::spawn(async move { read_stream_and_write_file(s2, rev_file_name_c, file_length).await });
                w_handler.await;
                r_handler.await;
                let sec = start.elapsed().as_secs() as usize;
                log::info!("stream={:?} readwrite cost: {:?} s", stream, sec);
                let _ = stream.close2().await;

                //let diff_r = diff(&path, &rev_file_name).await;
                //assert_eq!(diff_r, true);
                let _ = fs::remove_file(&path).await;
                let _ = fs::remove_file(&rev_file_name).await;
            });
            handles.push(handle);
        }
        for handle in handles {
            handle.await;
        }
        let sec = start.elapsed().as_secs() as usize;
        log::info!("total cost: {:?} s", sec);

        let _ = control.close().await;
        task::sleep(Duration::from_secs(2)).await;
    });
}

async fn read_file_and_write_stream<C>(mut stream: C, mut file: File)
where
    C: ReadEx + WriteEx,
{
    let mut buf = [0; 4096];
    let mut r_total = 0;
    while let Ok(n) = file.read2(&mut buf).await {
        if n == 0 {
            break;
        }
        r_total += n;
        stream.write_all2(&buf[0..n]).await.unwrap();
        log::trace!("file to stream,  length={}", n);
    }
    let _ = file.close().await;
    log::info!("file to stream: total length={}", r_total);
}

async fn read_stream_and_write_file<C>(mut stream: C, rev_file_name: String, file_length: usize)
where
    C: ReadEx + WriteEx,
{
    log::info!("stream  to file begin");
    let mut buf = [0; 4096];
    let mut w_total = 0;
    let mut r_total = 0;
    let mut rev_file = File::create(&rev_file_name).await.unwrap();
    while let Ok(n) = stream.read2(&mut buf).await {
        log::trace!(" read stream data={}", n);
        if n == 0 {
            break;
        }
        r_total += n;
        let output = select(rev_file.write(&buf[0..n]), futures_timer::Delay::new(Duration::from_secs(10))).await;
        let r = match output {
            Either::Left((w, _)) => w,
            Either::Right(_) => Err(std::io::ErrorKind::TimedOut.into()),
        };
        let w_count = r.unwrap();
        w_total += w_count;
        if r_total >= file_length {
            break;
        }
    }
    let _ = rev_file.flush().await;
    let _ = rev_file.close().await;
    log::info!(
        "stream  to file end,origin file length={},read length={},write length={}",
        file_length,
        r_total,
        w_total
    );
    if r_total != w_total {
        panic!("data miss");
    }
}

pub async fn generate_rand_file(file_size: usize) -> String {
    log::info!(" generate file begin");
    let rng = rand::thread_rng();
    let file_name = rng.sample_iter(&Alphanumeric).take(10).collect::<String>();
    let file_path = format!("libp2p_test_{}", file_name);
    let mut count: usize = 0;
    let mut tmp_file = File::create(&file_path).await.unwrap();
    let data = vec![0x1; 1000 * 1000];
    while count < file_size {
        count += 1;
        let _ = tmp_file.write(&data[..]).await;
    }
    let _ = tmp_file.flush().await;
    let _ = tmp_file.close().await;
    log::info!(" generate file end");
    file_path
}

/// Takes two string filepaths and returns true if the two files are identical and exist.
pub async fn diff(f1: &str, f2: &str) -> bool {
    let d1 = digest(f1).await;
    let d2 = digest(f2).await;
    d1.as_ref() == d2.as_ref()
}

async fn digest(path: &str) -> md5::Digest {
    let mut file = File::open(path).await.unwrap();
    let mut ctx = md5::Context::new();
    let mut buffer = [0u8; 4096];
    loop {
        let n = file.read2(&mut buffer).await.unwrap();
        ctx.consume(&buffer[..n]);
        if n == 0 || n < 4096 {
            break;
        }
    }
    ctx.compute()
}
