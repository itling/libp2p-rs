package main

import (
	//"bufio"
	"context"
	"crypto/md5"
	"crypto/rand"
	"encoding/hex"
	//"flag"
	"fmt"
	"io"
	"log"
	ra "math/rand"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
	//"io/ioutil"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p-core/crypto"
	"github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/peerstore"

	//mplex "github.com/libp2p/go-libp2p-mplex"
	secio "github.com/libp2p/go-libp2p-secio"
	yamux "github.com/libp2p/go-libp2p-yamux"
	tcp "github.com/libp2p/go-tcp-transport"
	ws "github.com/libp2p/go-ws-transport"
	"github.com/multiformats/go-multiaddr"
)

func main() {
	// client server
	var clientOrserver string = os.Args[1]
	var r io.Reader
	if clientOrserver == "server" {
		// Creates a  fixed ED25519 key pair for this host.
		var ed25519Fixed []byte
		for i:=0;i<32;i++{
			ed25519Fixed=append(ed25519Fixed,0)
		}
		r = strings.NewReader(string(ed25519Fixed[:]))
	} else if clientOrserver == "client"  {
		r = rand.Reader
	} else {
		panic("param error")
	}

	prvKey, _, err := crypto.GenerateKeyPairWithReader(crypto.Ed25519, 32, r)
	if err != nil {
		panic(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	transports:=libp2p.ChainOptions(
		libp2p.Transport(tcp.NewTCPTransport),
		libp2p.Transport(ws.New),
	)

	muxers:=libp2p.ChainOptions(
		libp2p.Muxer("/yamux/1.0.0", yamux.DefaultTransport),
		//libp2p.Muxer("/mplex/6.7.0", mplex.DefaultTransport),
	)

	libp2p.Security(secio.ID, secio.New)
	noSecurity:=libp2p.NoSecurity

	// 0.0.0.0 will listen on any interface device.
	var sourcePort int = 38889 //for client
	var listenPort int = sourcePort
	var wsListenPort int = listenPort+1
	if clientOrserver == "client" {
		listenPort=0
		wsListenPort=0
	}
	sourceMultiAddr1, _ := multiaddr.NewMultiaddr(fmt.Sprintf("/ip4/0.0.0.0/tcp/%d", listenPort))
	sourceMultiAddr2, _ := multiaddr.NewMultiaddr(fmt.Sprintf("/ip4/0.0.0.0/tcp/%d/ws", wsListenPort))	


	host, err := libp2p.New(
		ctx,
		transports,
		libp2p.ListenAddrs(sourceMultiAddr1,sourceMultiAddr2),
		muxers,
		//security,
		noSecurity,
		libp2p.Identity(prvKey),
	)

	if err != nil {
		panic(err)
	}

	if clientOrserver == "server" {

		host.SetStreamHandler("/filetrans/1.0.0", handleStream)

		var port string
		for _, la := range host.Network().ListenAddresses() {
			if p, err := la.ValueForProtocol(multiaddr.P_TCP); err == nil {
				port = p
				break
			}
		}

		if port == "" {
			panic("was not able to find actual local port")
		}

		fmt.Printf("Run './file_transfer client ' on another console.[port=%s, peerID=%s]\n", port, host.ID().Pretty())
		fmt.Printf("\nWaiting for incoming connection\n\n")

		// Hang forever
		<-make(chan struct{})
	}else if clientOrserver == "client" {
		fmt.Println("This node's multiaddresses:")
		for _, la := range host.Addrs() {
			fmt.Printf(" - %v\n", la)
		}
		fmt.Println()

		var mas []multiaddr.Multiaddr
		maddr1, _ := multiaddr.NewMultiaddr(fmt.Sprintf("/ip4/127.0.0.1/tcp/%d", sourcePort))
		mas = append(mas, maddr1)
		// maddr2, _ := multiaddr.NewMultiaddr(fmt.Sprintf("/ip4/127.0.0.1/tcp/%d/ws", sourcePort+1))
		// mas = append(mas, maddr2)

		peerID, _ := peer.IDB58Decode("12D3KooWDpJ7As7BWAwRMfu1VU2WCqNjvq387JEYKDBj4kx6nXTN")
		host.Peerstore().AddAddrs(peerID, mas, peerstore.PermanentAddrTTL)

		fileName, fileSize := generateRandFile(20000)
		var wgStream sync.WaitGroup
		start := time.Now()
		for i := 0; i < 1; i++ {
			wgStream.Add(1)
			go func(k int) {
				
				defer wgStream.Done()
				//log.Printf("peerID=%s addr=%s ", peerID,mas)
				s, err := host.NewStream(context.Background(), peerID, "/filetrans/1.0.0")
				defer s.Close()
				if err != nil {
					panic(err)
				}
				revFileName := fileName + "-rev-" + strconv.Itoa(k)
				var wg sync.WaitGroup
				wg.Add(2)
				innerStart := time.Now()
				go readFileAndWriteStream(s, fileName, &wg)
				go readStreamAndWriteFile(s, revFileName, fileSize, &wg)
				wg.Wait()
				cost := time.Now().Sub(innerStart)
				log.Printf("stream=%v readwrite cost time=%s", k, cost)
				s.Close()
				// if HashFileMd5(fileName) == HashFileMd5(revFileName) {
				// 	log.Printf("stream=%v  test pass!", k)
				// } else {
				// 	log.Printf("stream=%v  test not pass!", k)
				// }
				err = os.Remove(revFileName)
			}(i)
		}
		wgStream.Wait()
		cost := time.Now().Sub(start)
		log.Printf("test total cost time=%s", cost)
		err = os.Remove(fileName)
	}
}

func handleStream(s network.Stream) {
	log.Println("Got a new stream!")
	go echo(s)
}

func echo(s network.Stream) {
	rootContext := context.Background()
	ctx, cancelFunc := context.WithCancel(rootContext)
	buf := make([]byte, 4096)
	var wTotal int = 0
	var rTotal int = 0
	startTime := time.Now()
	go printTransRate(ctx, s, &wTotal, &wTotal)
	for {
		n, err := s.Read(buf)
		if err != nil {
			endTime := time.Now()
			sec := endTime.Sub(startTime).Seconds()
			rate := (rTotal / 1024) / int(sec)
			log.Printf("stream:%s: total bytes=%d,cost=%ds, avg rate=%vkb/s ", s.Conn(),rTotal,int(sec), rate)

			cancelFunc()
			log.Printf("stream [%s] read error:%s", s.Conn(), err)
			break
		}
		rTotal += n
		n, err = s.Write(buf[0:n])
		if err != nil {
			cancelFunc()
			log.Printf("stream [%s] write error:%s", s.Conn(), err)
			break
		}
		wTotal += n
	}

}

func printTransRate(ctx context.Context, s network.Stream, rTotal *int, wTotal *int) {
	startTime := time.Now()
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	lastRTotal:=0;
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			endTime := time.Now()
			sec := endTime.Sub(startTime).Seconds()
			if lastRTotal==*rTotal{
				log.Printf("stream:%s: total bytes=%d,  rate=%vkb/s ", s.Conn(),*rTotal, 0)
			}else{
				rate := (*rTotal / 1024) / int(sec)
				log.Printf("stream:%s: total bytes=%d,  rate=%vkb/s ", s.Conn(),*rTotal, rate)
			}
			lastRTotal=*rTotal
		}
	}
}

func readFileAndWriteStream(s network.Stream, fileName string, wg *sync.WaitGroup) {
	defer wg.Done()
	f, err := os.Open(fileName)
	if err != nil {
		panic(err)
	}
	defer f.Close()
	var rTotal int = 0
	var wTotal int = 0
	buf := make([]byte, 4096)
	for {
		n, err := f.Read(buf)
		if err != nil && err != io.EOF {
			panic(err)
		}
		if n == 0 {
			break
		}
		rTotal += n
		n, err = s.Write(buf[0:n])
		if err != nil {
			panic(err)
		}
		wTotal += n
	}
	log.Printf("readFileAndWriteStream read bytes=%v , write bytes=%v\n", rTotal, wTotal)

}

func readStreamAndWriteFile(s network.Stream, revFileName string, fileLength int, wg *sync.WaitGroup) {
	defer wg.Done()
	f, err := os.Create(revFileName)
	if err != nil {
		panic(err)
	}
	defer f.Close()
	buf := make([]byte, 4096)
	var wTotal int = 0
	var rTotal int = 0
	for {
		n, err := s.Read(buf)
		if err != nil && err != io.EOF {
			panic(err)
		}
		rTotal += n
		if n == 0 {
			break
		}
		// n, err = f.Write(buf[0:n])
		// if err != nil {
		// 	panic(err)
		// }
		// wTotal += n
		if rTotal >= fileLength {
			break
		}
	}

	log.Printf("readStreamAndWriteFile read bytes=%v  , write bytes=%v , file total length=%v\n", rTotal, wTotal, fileLength)

}

func generateRandFile(fileSize int) (string, int) {
	var data []byte
	for i := 0; i <= 1000*1000; i++ {
		data = append(data, 0x1)
	}
	fileName := RandFileName(10)
	f, _ := os.Create(fileName)
	defer f.Close()
	var count int = 0
	var total int = 0
	for {
		count = count + 1
		n, _ := f.Write(data)
		total += n
		if count > fileSize {
			break
		}
	}
	return fileName, total
}

// RandFileName
func RandFileName(nameLen int) string {
	r := ra.New(ra.NewSource(time.Now().UnixNano()))
	bytes := make([]byte, nameLen)
	for i := 0; i < nameLen; i++ {
		b := r.Intn(26) + 65
		bytes[i] = byte(b)
	}
	return "libp2p_test_" + string(bytes)
}

//HashFileMd5 return md5 value of file
func HashFileMd5(filePath string) (md5Str string) {
	file, err := os.Open(filePath)
	if err != nil {
		panic(err)
	}
	defer file.Close()
	hash := md5.New()
	if _, err = io.Copy(hash, file); err != nil {
		panic(err)
	}
	hashInBytes := hash.Sum(nil)[:16]
	md5Str = hex.EncodeToString(hashInBytes)
	//log.Printf("%s md5= %s",filePath,md5Str)
	return
}
