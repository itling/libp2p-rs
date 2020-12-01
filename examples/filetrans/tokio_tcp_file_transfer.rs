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

async fn print_trans_rate(r_total: Arc<AtomicUsize>) {
    let start = Instant::now();
    let mut last_r_total = 0;
    loop {
        Delay::new(Duration::from_secs(5)).await;
        let sec = start.elapsed().as_secs() as usize;
        let data = r_total.load(Ordering::SeqCst);
        if data == last_r_total {
            log::info!("total bytes={}, rate={}kb/s ", data, 0);
        } else {
            let rate = (data / 1024) / sec;
            log::info!("total bytes={}, rate={}kb/s ", data, rate);
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
                let mut buf = [0; 40960];
                let r_total = Arc::new(AtomicUsize::new(0));
                let r_total_c = r_total.clone();
                let rate_handler = task::spawn(async move { print_trans_rate(r_total_c).await });
                while let Ok(n) = socket.read(&mut buf[..]).await {
                    if n == 0 {
                        break;
                    }
                    r_total.fetch_add(n, Ordering::SeqCst);
                    socket.write_all(&buf[0..n]).await.unwrap();
                }
                rate_handler.abort();
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
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let path = generate_rand_file(max_length).await;
        let file = File::open(&path).await.unwrap();
        let file_length = file.metadata().await.unwrap().len() as usize;
        log::info!("stream opened, read file [{}],length={}", &path, file_length);
        let stream = TcpStream::connect(&format!("127.0.0.1:{}", port)).await.unwrap();
        let (read_half, write_half) = stream.into_split();
        let start = Instant::now();
        let w_handler = task::spawn(async { read_file_and_write_stream(write_half, file).await });
        let rev_file_name = format!("{}-rev-{}", &path, 0);
        let rev_file_name_c = rev_file_name.clone();
        let r_handler = task::spawn(async move { read_stream_and_write_file(read_half, rev_file_name_c, file_length).await });
        let _ = w_handler.await;
        let _ = r_handler.await;
        let sec = start.elapsed();
        log::info!("readwrite cost: {:?}", sec);
        let _ = fs::remove_file(&path).await;
        let _ = fs::remove_file(&rev_file_name).await;
        sleep(Duration::from_secs(2)).await;
    });
}

async fn read_file_and_write_stream(mut stream: OwnedWriteHalf, mut file: File) {
    let mut buf = [0; 40960];
    let mut r_total = 0;
    while let Ok(n) = file.read(&mut buf).await {
        if n == 0 {
            break;
        }
        r_total += n;
        stream.write_all(&buf[0..n]).await.unwrap();
        log::trace!("file to stream,  length={}", n);
    }
    let _ = file.flush().await;
    log::info!("file to stream: total length={}", r_total);
}

async fn read_stream_and_write_file(mut stream: OwnedReadHalf, rev_file_name: String, file_length: usize) {
    log::info!("stream  to file begin");
    let mut buf = [0; 40960];
    let mut r_total = 0;
    let mut rev_file = File::create(&rev_file_name).await.unwrap();
    while let Ok(n) = stream.read(&mut buf).await {
        log::trace!(" read stream data={}", n);
        if n == 0 {
            break;
        }
        r_total += n;
        if r_total >= file_length {
            break;
        }
    }
    let _ = rev_file.flush().await;
    log::info!("stream  to file end,origin file length={},read length={}", file_length, r_total);
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
        let _ = tmp_file.write_all(&data[..]).await;
    }
    let _ = tmp_file.flush().await;
    log::info!(" generate file end");
    file_path
}
