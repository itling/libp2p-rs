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

use async_std::net::{TcpListener, TcpStream};
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
        run_client(38889, 20000);
    } else {
        panic!("param error")
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

fn run_server(port: u32) {
    task::block_on(async {
        let listener = TcpListener::bind(&format!("127.0.0.1:{}", port)).await.unwrap();
        while let Ok((mut socket, _)) = listener.accept().await {
            task::spawn(async move {
                let start = Instant::now();
                let mut buf = [0; 40960];
                let r_total = Arc::new(AtomicUsize::new(0));
                let sc = socket.clone();
                let r_total_c = r_total.clone();
                let rate_handler = task::spawn(async move { print_trans_rate(sc, r_total_c).await });
                while let Ok(n) = socket.read2(&mut buf[..]).await {
                    if n == 0 {
                        break;
                    }
                    r_total.fetch_add(n, Ordering::SeqCst);
                    socket.write_all2(&buf[0..n]).await.unwrap();
                }
                rate_handler.cancel().await;
                let sec = start.elapsed().as_secs() as usize;
                let data = r_total.load(Ordering::SeqCst);
                let rate = (data / 1024) / sec;
                log::info!("stream:{:?} , total bytes={}, avg rate={}kb/s ", socket, data, rate);
            })
            .await;
        }
    });
}

fn run_client(port: u32, max_length: usize) {
    task::block_on(async {
        let path = generate_rand_file(max_length).await;
        let file = File::open(&path).await.unwrap();
        let file_length = file.metadata().await.unwrap().len() as usize;
        log::info!("stream opened, read file [{}],length={}", &path, file_length);
        let start = Instant::now();
        let mut stream = TcpStream::connect(&format!("127.0.0.1:{}", port)).await.unwrap();
        let s1 = stream.clone();
        let w_handler = task::spawn(async { read_file_and_write_stream(s1, file).await });

        let s2 = stream.clone();
        let rev_file_name = format!("{}-rev-{}", &path, 0);
        let rev_file_name_c = rev_file_name.clone();
        let r_handler = task::spawn(async move { read_stream_and_write_file(s2, rev_file_name_c, file_length).await });
        w_handler.await;
        r_handler.await;
        let sec = start.elapsed().as_secs() as usize;
        log::info!("stream={:?} readwrite cost: {:?} s", stream, sec);
        let _ = stream.close().await;
        let _ = fs::remove_file(&path).await;
        let _ = fs::remove_file(&rev_file_name).await;
        task::sleep(Duration::from_secs(2)).await;
    });
}

async fn read_file_and_write_stream<C>(mut stream: C, mut file: File)
where
    C: ReadEx + WriteEx,
{
    let mut buf = [0; 40960];
    let mut r_total = 0;
    while let Ok(n) = file.read2(&mut buf).await {
        if n == 0 {
            break;
        }
        r_total += n;
        stream.write_all2(&buf[0..n]).await.unwrap();
        log::trace!("file to stream,  length={}", n);
    }
    let _ = file.flush().await;
    let _ = file.close().await;
    log::info!("file to stream: total length={}", r_total);
}

async fn read_stream_and_write_file<C>(mut stream: C, rev_file_name: String, file_length: usize)
where
    C: ReadEx + WriteEx,
{
    log::info!("stream  to file begin");
    let mut buf = [0; 40960];
    let mut w_total = 0;
    let mut r_total = 0;
    let mut rev_file = File::create(&rev_file_name).await.unwrap();
    while let Ok(n) = stream.read2(&mut buf).await {
        log::trace!(" read stream data={}", n);
        if n == 0 {
            break;
        }
        r_total += n;
        // let output = select(rev_file.write(&buf[0..n]), futures_timer::Delay::new(Duration::from_secs(10))).await;
        // let r = match output {
        //     Either::Left((w, _)) => w,
        //     Either::Right(_) => Err(std::io::ErrorKind::TimedOut.into()),
        // };
        // let w_count = r.unwrap();
        // w_total += w_count;
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
    // if r_total != w_total {
    //     panic!("data miss");
    // }
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
