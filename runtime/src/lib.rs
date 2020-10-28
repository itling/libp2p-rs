#[cfg(not(any(feature = "runtime-tokio", feature = "runtime-async-std")))]
compile_error!("one of 'runtime-async-std' or 'runtime-tokio' features must be enabled");

//#[cfg(all(feature = "runtime-tokio", feature = "runtime-async-std"))]
//compile_error!("only one of 'runtime-async-std' or 'runtime-tokio' features must be enabled");
#[cfg(feature = "runtime-tokio")]
use std::{
    pin::Pin,
    task::{Context, Poll},
};

#[cfg(feature = "runtime-async-std")]
pub mod task {
    pub use async_std::task::{block_on, sleep, spawn, JoinHandle};
}

#[cfg(feature = "runtime-async-std")]
pub mod io {
    pub use async_std::io::{stdin, stdout, timeout};
}

#[cfg(feature = "runtime-async-std")]
pub use async_std::net::{TcpListener, TcpStream, ToSocketAddrs};

#[cfg(feature = "runtime-tokio")]
pub mod task {
    pub use tokio::task::{spawn, spawn_blocking, JoinHandle};
    pub use tokio::time::delay_for as sleep;
    //pub use tokio::time::sleep;

    // pub fn spawn<T>(task: T) -> tokio::task::JoinHandle<T::Output>
    // where
    //     T:  std::future::Future + Send + 'static,
    //     T::Output: Send + 'static,
    // {
    //     let mut rt=tokio::runtime::Runtime::new().unwrap();
    //     rt.spawn(task)
    // }

    pub fn block_on<F: std::future::Future>(future: F) -> F::Output {
        let mut rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(future)
    }
}

#[cfg(feature = "runtime-tokio")]
pub mod io {
    pub use tokio::io::{stdin, stdout, AsyncRead, AsyncWrite};
    //pub use tokio::io::ReadBuf;
    pub use tokio::time::timeout;
}

#[cfg(feature = "runtime-tokio")]
pub use tokio::{
    net::{TcpListener, TcpStream, ToSocketAddrs},
    runtime::Runtime,
};

#[cfg(feature = "runtime-tokio")]
pub struct TokioTcpStream {
    inner: TcpStream,
}
impl TokioTcpStream {
    pub fn new(inner: TcpStream) -> Self {
        Self { inner }
    }
}

#[cfg(feature = "runtime-tokio")]
impl futures::io::AsyncRead for TokioTcpStream {
    fn poll_read(mut self: Pin<&mut Self>, cx: &mut Context, buf: &mut [u8]) -> Poll<Result<usize, std::io::Error>> {
        tokio::io::AsyncRead::poll_read(Pin::new(&mut self.inner), cx, buf)
        // let mut bf=tokio::io::ReadBuf::new(buf);
        // let result=tokio::io::AsyncRead::poll_read(Pin::new(&mut self.inner), cx, &mut bf);
        // match result{
        //     Poll::Ready(r)=>{
        //         match r{
        //             Ok(_)=>{
        //                 Poll::Ready(Ok(bf.filled().len()))
        //             },
        //             Err(e)=>{
        //                 Poll::Ready(Err(e))
        //             },
        //         }
        //     },
        //     Poll::Pending=>Poll::Pending
        // }
    }
}
#[cfg(feature = "runtime-tokio")]
impl futures::io::AsyncWrite for TokioTcpStream {
    fn poll_write(mut self: Pin<&mut Self>, cx: &mut Context, buf: &[u8]) -> Poll<Result<usize, std::io::Error>> {
        tokio::io::AsyncWrite::poll_write(Pin::new(&mut self.inner), cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<(), std::io::Error>> {
        tokio::io::AsyncWrite::poll_flush(Pin::new(&mut self.inner), cx)
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<(), std::io::Error>> {
        tokio::io::AsyncWrite::poll_shutdown(Pin::new(&mut self.inner), cx)
    }
}
