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

use futures_timer::Delay;
use rand::distributions::Alphanumeric;
use rand::Rng;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant};
use structopt::StructOpt;
use tokio::fs::{self, *};
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::{self};
use tokio::time::sleep;
use tokio::io::{BufWriter,BufReader};
use bytes::BytesMut;
use tokio::io::BufStream;

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
        run_client(38889, 100000);
    } else {
        panic!("param error")
    }
}

async fn print_trans_rate(r_total: Arc<AtomicUsize>) {
    let start = Instant::now();
    let mut last_r_total = 0;
    loop {
        Delay::new(Duration::from_secs(5)).await;
        let sec = start.elapsed().as_secs() as usize;
        let data = r_total.load(Ordering::SeqCst);
        if data == last_r_total {
            log::info!("total bytes={}, rate={}M/s ", data, 0);
        } else {
            let rate = (data / 1024/1024) / sec;
            log::info!("total bytes={}, rate={}M/s ", data, rate);
        }
        last_r_total = data
    }
}

fn run_server(port: u32) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let listener = TcpListener::bind(&format!("127.0.0.1:{}", port)).await.unwrap();
        while let Ok((mut socket, _)) = listener.accept().await {
            let _ = task::spawn(async move {
                let start = Instant::now();
                let mut buf = [0; 4096];
                let r_total = Arc::new(AtomicUsize::new(0));
                let r_total_c = r_total.clone();
                let rate_handler = task::spawn(async move { print_trans_rate(r_total_c).await });
                let (read_half, write_half) = socket.into_split();
                let mut buf_r=BufReader::with_capacity(4096,read_half);
                let mut buf_w=BufWriter::with_capacity(4096,write_half);
                loop{
                    buf_r.read_exact(&mut buf).await.unwrap();
                    r_total.fetch_add(buf.len(), Ordering::SeqCst);
                    buf_w.write_all(&buf[..]).await.unwrap();
                }
                rate_handler.abort();
                let sec = start.elapsed().as_secs() as usize;
                let data = r_total.load(Ordering::SeqCst);
                let rate = (data / 1024/1024) / sec;
                log::info!("total bytes={}, avg rate={}M/s ", data, rate);
            })
            .await;
        }
    });
}

fn run_client(port: u32, max_length: usize) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let stream = TcpStream::connect(&format!("127.0.0.1:{}", port)).await.unwrap();
      
        let start = Instant::now();
        // let (read_half, write_half) = stream.into_split();
        // let w_handler = task::spawn(async move{ write_stream(write_half, max_length).await });
        // let r_handler = task::spawn(async move { read_stream(read_half,max_length).await });
        // let _ = w_handler.await;
        // let _ = r_handler.await;
        read_write_stream(stream,max_length).await;
        let sec = start.elapsed();
        log::info!("readwrite cost: {:?}", sec);
        sleep(Duration::from_secs(2)).await;
    });
}

async fn read_write_stream(mut stream: TcpStream, max_length: usize)
{
    let mut buf = [0; 4096];
    let mut w_total = 0;
    let mut r_total = 0;
    let (read_half, write_half) = stream.into_split();
    let mut buf_r=BufReader::with_capacity(4096,read_half);
    let mut buf_w=BufWriter::with_capacity(4096,write_half);
    loop {
        buf_w.write_all(&buf[..]).await.unwrap();
        w_total += buf.len();
        buf_r.read_exact(&mut buf).await.unwrap();
        if w_total >= max_length*1000*1000 {
            break;
        }
    }
    log::info!("write to stream: total length={}", w_total);
}

async fn write_stream(mut stream: OwnedWriteHalf,  max_length: usize) {
    let mut buf = [0; 40960];
    let mut w_total = 0;
    let mut buf_w=BufWriter::new(stream);
    loop {
        buf_w.write_all(&buf[..]).await.unwrap();
        w_total += buf.len();
        if w_total >= max_length*1000*1000 {
            break;
        }
    }
    log::info!("write to stream: total length={}", w_total);
}

async fn read_stream(mut stream: OwnedReadHalf, max_length: usize) {
    log::info!("read stream  begin");
    let mut buf = [0; 40960];
    let mut r_total = 0;
    let mut buf_r=BufReader::new(stream);
    loop {
        buf_r.read_exact(&mut buf).await.unwrap();
        r_total += buf.len();
        if r_total >= max_length*1000*1000 {
            break;
        }
    }
    log::info!(
        "read stream  end,max length={},read length={}",
        max_length,
        r_total,
    );
}
