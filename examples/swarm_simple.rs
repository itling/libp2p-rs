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

//use async_std::task;
use async_trait::async_trait;
use libp2prs_core::runtime::{task, Runtime};
use std::time::Duration;
#[macro_use]
extern crate lazy_static;

use libp2prs_core::identity::Keypair;
use libp2prs_core::transport::memory::MemoryTransport;
use libp2prs_core::transport::upgrade::TransportUpgrade;
use libp2prs_core::upgrade::UpgradeInfo;
use libp2prs_core::{Multiaddr, PeerId};
use libp2prs_secio as secio;
use libp2prs_swarm::identify::IdentifyConfig;
use libp2prs_swarm::ping::PingConfig;
use libp2prs_swarm::protocol_handler::{IProtocolHandler, ProtocolHandler};
use libp2prs_swarm::substream::Substream;
use libp2prs_swarm::{DummyProtocolHandler, Swarm, SwarmError};
use libp2prs_tcp::TcpConfig;
use libp2prs_traits::{ReadEx, WriteEx};
use libp2prs_yamux as yamux;

fn main() {
    env_logger::from_env(env_logger::Env::default().default_filter_or("info")).init();
    if std::env::args().nth(1) == Some("server".to_string()) {
        log::info!("Starting server ......");
        run_server();
    } else {
        log::info!("Starting client ......");
        run_client();
    }
}

lazy_static! {
    static ref SERVER_KEY: Keypair = Keypair::generate_ed25519_fixed();
}

#[allow(clippy::empty_loop)]
fn run_server() {
    let keys = SERVER_KEY.clone();

    let listen_addr: Multiaddr = "/ip4/127.0.0.1/tcp/8086".parse().unwrap();
    let sec = secio::Config::new(keys.clone());
    let mux = yamux::Config::new();
    //let mux = mplex::Config::new();
    //let mux = Selector::new(yamux::Config::new(), mplex::Config::new());
    let tu = TransportUpgrade::new(TcpConfig::default(), mux.clone(), sec.clone());
    let tu2 = TransportUpgrade::new(MemoryTransport::default(), mux, sec);

    #[derive(Clone)]
    struct MyProtocolHandler;

    impl UpgradeInfo for MyProtocolHandler {
        type Info = &'static [u8];

        fn protocol_info(&self) -> Vec<Self::Info> {
            vec![b"/my/1.0.0"]
        }
    }

    #[async_trait]
    impl ProtocolHandler for MyProtocolHandler {
        async fn handle(&mut self, stream: Substream, _info: <Self as UpgradeInfo>::Info) -> Result<(), SwarmError> {
            let mut stream = stream;
            log::trace!("MyProtocolHandler handling inbound {:?}", stream);
            let mut msg = vec![0; 4096];
            loop {
                let n = stream.read2(&mut msg).await?;
                log::info!("received: {:?}", &msg[..n]);
                stream.write2(&msg[..n]).await?;
            }
        }

        fn box_clone(&self) -> IProtocolHandler {
            Box::new(self.clone())
        }
    }

    let mut swarm = Swarm::new(keys.public())
        .with_transport(Box::new(tu))
        .with_transport(Box::new(tu2))
        .with_protocol(Box::new(DummyProtocolHandler::new()))
        .with_protocol(Box::new(MyProtocolHandler))
        .with_ping(PingConfig::new().with_unsolicited(true).with_interval(Duration::from_secs(1)))
        .with_identify(IdentifyConfig::new(false));

    log::info!("Swarm created, local-peer-id={:?}", swarm.local_peer_id());

    let _control = swarm.control();
    let rt = Runtime::new().unwrap();
    rt.spawn(async move {
        swarm.listen_on(vec![listen_addr]).unwrap();
        swarm.start();
    });

    loop {
        std::thread::sleep(std::time::Duration::from_secs(5));
    }
}

fn run_client() {
    let keys = Keypair::generate_secp256k1();

    let _addr: Multiaddr = "/ip4/127.0.0.1/tcp/8086".parse().unwrap();
    let sec = secio::Config::new(keys.clone());
    let mux = yamux::Config::new();
    //let mux = Selector::new(yamux::Config::new(), mplex::Config::new());
    let tu = TransportUpgrade::new(TcpConfig::default(), mux, sec);

    let mut swarm = Swarm::new(keys.public())
        .with_transport(Box::new(tu))
        .with_ping(PingConfig::new().with_unsolicited(false).with_interval(Duration::from_secs(1)))
        .with_identify(IdentifyConfig::new(false));

    let mut control = swarm.control();

    let remote_peer_id = PeerId::from_public_key(SERVER_KEY.public());

    log::info!("about to connect to {:?}", remote_peer_id);

    swarm.peer_addrs_add(&remote_peer_id, "/ip4/127.0.0.1/tcp/8086".parse().unwrap(), Duration::default());

    let rt = Runtime::new().unwrap();
    rt.spawn(async {
        swarm.start();
    });
    task::block_on(async move {
        control.new_connection(remote_peer_id.clone()).await.unwrap();
        let mut stream = control.new_stream(remote_peer_id, vec![b"/my/1.0.0"]).await.unwrap();

        log::info!("stream {:?} opened, writing something...", stream);

        let _ = stream.write_all2(b"hello").await;

        task::sleep(Duration::from_secs(10)).await;

        let _ = stream.close2().await;

        log::info!("shutdown is completed");
    });
}
