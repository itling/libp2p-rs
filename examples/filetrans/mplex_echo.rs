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

use async_std::{
    net::{TcpListener, TcpStream},
    task,
};
//use libp2prs_mplex::connection::Connection;
use libp2prs_yamux::{connection::Connection, connection::Mode, Config};

use libp2prs_traits::{ ReadEx, WriteEx};
use log::info;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};
use structopt::StructOpt;
use async_std::io::{BufWriter,BufReader,Read,Write};
use futures_timer::Delay;
use futures::{AsyncRead,AsyncWrite};
use pin_project_lite::pin_project;

use futures::{
    io::{IoSliceMut},
    prelude::*,
};
use std::{io, io::Error as IoError, pin::Pin, task::Context, task::Poll};
use std::fmt;
use std::io::prelude::*;
use std::error;


fn main() {
    env_logger::from_env(env_logger::Env::default().default_filter_or("info")).init();
    if std::env::args().nth(1) == Some("server".to_string()) {
        info!("Starting server ......");
        run_server();
    } else {
        info!("Starting client ......");
        run_client();
    }
}

pin_project! {
    struct BufStream
    {
        #[pin]
        reader: BufReader<&'static TcpStream>,
        #[pin]
        writer: BufWriter<&'static TcpStream>,
        _socket: Pin<Box<TcpStream>>,
    }
}
impl BufStream{
    pub fn new(socket: TcpStream) -> Self {
        let pin = Box::pin(socket);
        unsafe {
            Self{
                reader : BufReader::with_capacity(64*1024,std::mem::transmute(&*pin)),
                writer: BufWriter::with_capacity(64*1024,std::mem::transmute(&*pin)),
                _socket: pin,
            }
        }
    }
}
impl AsyncRead for BufStream {
    fn poll_read(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut [u8]) -> Poll<Result<usize, IoError>> {
        self.project().reader.poll_read(cx, buf)
    }

    fn poll_read_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &mut [IoSliceMut<'_>],
    ) -> Poll<io::Result<usize>> {
        self.project().reader.poll_read_vectored(cx, bufs)
    }
}
impl AsyncWrite for BufStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.project().writer.poll_write(cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.project().writer.poll_flush(cx)
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.project().writer.poll_close(cx)
    }
}

impl AsyncBufRead for BufStream {
    fn poll_fill_buf(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<&[u8]>> {
        self.project().reader.poll_fill_buf(cx)
    }

    fn consume(self: Pin<&mut Self>, amt: usize) {
        self.project().reader.consume(amt)
    }
}


fn run_server() {
    task::block_on(async {
        let listener = TcpListener::bind("127.0.0.1:8088").await.unwrap();
        while let Ok((socket, _)) = listener.accept().await {
            let rw=BufStream::new(socket);
            task::spawn(async move {
                let muxer_conn = Connection::new(rw,Config::default(),Mode::Server);
                //let muxer_conn = Connection::new(rw);
                let mut ctrl = muxer_conn.control();

                task::spawn(async {
                    let mut muxer_conn = muxer_conn;
                    while muxer_conn.next_stream().await.is_ok() {}
                    info!("connection is closed");
                });

                while let Ok(stream) = ctrl.accept_stream().await {
                    info!("accepted new stream: {:?}", stream);
                    task::spawn(async move {
                        let start = Instant::now();
                        let mut r = stream.clone();
                        let mut  w = stream.clone();

                        let r_total = Arc::new(AtomicUsize::new(0));
                        let r_total_c = r_total.clone();
                        let mut buf = [0; 40960];
                        let rate_handler = task::spawn(async move { print_trans_rate(r_total_c).await });
                        
                        loop{
                            r.read_exact2(&mut buf).await.unwrap();
                            r_total.fetch_add(40960, Ordering::SeqCst);
                            w.write_all2(&buf[..]).await.unwrap();
                        }
                        let _=rate_handler.cancel().await;
                        let sec = start.elapsed().as_secs() as usize;
                        let data = r_total.load(Ordering::SeqCst);
                        let rate = (data / 1024/1024) / sec;
                        log::info!("total bytes={}, avg rate={}m/s ", data, rate);
                        
                    });
                }
            });
        }
    });
}
async fn print_trans_rate(r_total: Arc<AtomicUsize>)
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

fn run_client() {
    task::block_on(async {
        let socket = TcpStream::connect("127.0.0.1:8088").await.unwrap();
        let rw=BufStream::new(socket);
        let muxer_conn = Connection::new(rw,Config::default(),Mode::Client);
        //let muxer_conn = Connection::new(rw);

        let mut ctrl = muxer_conn.control();

        let loop_handle = task::spawn(async {
            let mut muxer_conn = muxer_conn;
            while muxer_conn.next_stream().await.is_ok() {}
            info!("connection is closed");
        });

        let mut stream = ctrl.clone().open_stream().await.unwrap();
        info!("C: opened new stream {}", stream.id());

        let mut writer = stream.clone();
        let mut reader = stream.clone();

        let data = vec![0x42; 40 * 1024];
        let c=task::spawn(async move {
            loop {
                writer.write_all2(&data[..]).await.unwrap();
            }
        });

        let s=task::spawn(async move {
            let mut frame = vec![0; 40960];
            loop{
                reader.read_exact2(&mut frame).await.unwrap();
            }
        });

        c.await;
        s.await;

        stream.close2().await.expect("close stream");

        ctrl.close().await.expect("close connection");

        loop_handle.await;

        info!("shutdown is completed");
    });
}
