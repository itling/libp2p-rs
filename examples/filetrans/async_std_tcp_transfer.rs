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
use async_std::io::{BufWriter,BufReader};
use async_std::fs::{self, *};
use futures_util::io::AsyncWriteExt;
use std::sync::Arc;

use async_std::net::{TcpListener, TcpStream};
use futures::prelude::*;
use futures_timer::Delay;

use libp2prs_traits::{ReadEx, WriteEx};

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
        run_client(38889, 200000);
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
            log::info!("total bytes={}, rate={}m/s ", data, 0);
        } else {
            let rate = (data / 1024/1024) / sec;
            log::info!(" total bytes={}, rate={}m/s ", data, rate);
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
                let mut buf = [0; 4096];
                let r_total = Arc::new(AtomicUsize::new(0));
                let sc = socket.clone();
                let r_total_c = r_total.clone();
                let rate_handler = task::spawn(async move { print_trans_rate(sc, r_total_c).await });
                let mut buf_r=BufReader::with_capacity(4096,socket.clone());
                let mut buf_w=BufWriter::with_capacity(4096,socket);
                loop{
                    buf_r.read_exact(&mut buf).await.unwrap();
                    r_total.fetch_add(4096, Ordering::SeqCst);
                    buf_w.write_all(&buf[..]).await.unwrap();
                }
                let _=rate_handler.cancel().await;
                let sec = start.elapsed().as_secs() as usize;
                let data = r_total.load(Ordering::SeqCst);
                let rate = (data / 1024/1024) / sec;
                log::info!("total bytes={}, avg rate={}m/s ", data, rate);
            })
            .await;
        }
    });
}

fn run_client(port: u32, max_length: usize) {
    task::block_on(async {
        let start = Instant::now();
        let mut stream = TcpStream::connect(&format!("127.0.0.1:{}", port)).await.unwrap();
         let s1 = stream.clone();
        // let w_handler = task::spawn(async move{ write_stream(s1,max_length).await });

        // let s2 = stream.clone();
        // let r_handler = task::spawn(async move{ read_stream(s2, max_length).await });
        // w_handler.await;
        // r_handler.await;
        read_write_stream(s1,max_length).await;
        let _ = stream.close().await;
        let sec = start.elapsed().as_secs() as usize;
        log::info!(" readwrite cost: {:?} s", sec);
        task::sleep(Duration::from_secs(2)).await;
    });
}



async fn read_write_stream(mut stream: TcpStream, max_length: usize)
{
    let mut buf = [0; 4096];
    let mut w_total = 0;
    let mut r_total = 0;
    let mut buf_r=BufReader::with_capacity(4096,stream.clone());
    let mut buf_w=BufWriter::with_capacity(4096,stream);
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

async fn write_stream(mut stream: TcpStream, max_length: usize)
{
    let mut buf_w=BufWriter::new(stream);
    let mut buf = [0; 40960];
    let mut w_total = 0;
    loop {
        buf_w.write_all(&buf[..]).await.unwrap();
        w_total += buf.len();
        if w_total >= max_length*1000*1000 {
            break;
        }
    }
    log::info!("write to stream: total length={}", w_total);
}


async fn read_stream(mut stream: TcpStream, max_length: usize)
{
    let mut buf_r=BufReader::new(stream);
    log::info!("read stream  begin");
    let mut buf = [0; 40960];
    let mut r_total = 0;
    loop {
        buf_r.read_exact(&mut buf).await.unwrap();
        r_total+= buf.len();
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
