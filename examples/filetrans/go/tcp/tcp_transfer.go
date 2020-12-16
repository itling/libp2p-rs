package main

import (
	"os"
	"context"
	"time"
	"net"
	"log"
	"io"
	"sync"
	"bufio"
)

const PACKAGE_SIZE = 10 * 4096

func main() {
	var clientOrserver string = os.Args[1]
	if clientOrserver == "server" {
		ln, err := net.Listen("tcp", "127.0.0.1:8080")
		if err != nil {
			panic(err)
		}
		for {
			stream, err := ln.Accept()
			if err != nil {
				panic(err)
			}
			go echo(stream)
		}

	}else if clientOrserver == "client" {
		s, err := net.Dial("tcp", "127.0.0.1:8080")
		if err != nil {
			panic(err)
		}
		start := time.Now()
		fileSize:=200000*1000*1000;//50g
		var wg sync.WaitGroup
		wg.Add(2)
		go writeStream(s,fileSize, &wg)
		go readStream(s, fileSize, &wg)
		wg.Wait()
		//writeReadStram(s,fileSize)
		cost := time.Now().Sub(start).Seconds()
		log.Printf("readwrite cost time=%ds",int(cost))
		s.Close()
	}
}


func echo(s  net.Conn) {
	rootContext := context.Background()
	ctx, cancelFunc := context.WithCancel(rootContext)
	buf := make([]byte, PACKAGE_SIZE)
	var wTotal int = 0
	var rTotal int = 0
	startTime := time.Now()
	go printTransRate(ctx, s, &wTotal, &wTotal)
	
	for {
		rw:=bufio.NewReadWriter(bufio.NewReader(s), bufio.NewWriter(s))
		//rw:=bufio.NewReadWriter(bufio.NewReaderSize(s,4096), bufio.NewWriterSize(s,4096))
		n, err := rw.Read(buf)
		if err != nil {
			endTime := time.Now()
			sec := endTime.Sub(startTime).Seconds()
			rate := (rTotal / 1024/1024) / int(sec)
			log.Printf("total bytes=%d, cost=%ds, avg rate=%vM/s ", rTotal,int(sec), rate)

			cancelFunc()
			log.Printf(" read error:%s",  err)
			break
		}
		rTotal += n
		n, err = rw.Write(buf[:])
		if err != nil {
			cancelFunc()
			log.Printf(" write error:%s", err)
			break
		}
		wTotal += n
		rw.Flush()
	}

}

func printTransRate(ctx context.Context, s  net.Conn, rTotal *int, wTotal *int) {
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
				log.Printf("rTotal bytes=%d,  rate=%vM/s ",*rTotal, 0)
			}else{
				rate := (*rTotal / 1024/1024) / int(sec)
				log.Printf("rTotal bytes=%d,  rate=%vM/s ",*rTotal, rate)
			}
			lastRTotal=*rTotal
		}
	}
}

func writeReadStram(s  net.Conn,fileLength int){

	var wTotal int = 0
	var rTotal int = 0
	buf := make([]byte, PACKAGE_SIZE)

	for {
		rw:=bufio.NewReadWriter(bufio.NewReader(s), bufio.NewWriter(s))
		//rw:=bufio.NewReadWriter(bufio.NewReaderSize(s,4096), bufio.NewWriterSize(s,4096))
		n, err := rw.Write(buf[:])
		if err != nil {
			panic(err)
		}
		rw.Flush();
		wTotal += n
		n2, err2 := rw.Read(buf)
		if err2 != nil{
			panic(err2)
		}
		if n2 == 0 {
			break
		}
		rTotal += n2
		if rTotal >= fileLength {
			break
		}
	}
	log.Printf("write bytes=%v read bytes=%v \n", wTotal,rTotal)

}

func writeStream(s  net.Conn,fileLength int, wg *sync.WaitGroup) {
	defer wg.Done()
	var wTotal int = 0
	buf := make([]byte, PACKAGE_SIZE)
	bs:=bufio.NewWriterSize(s,PACKAGE_SIZE);
	for {
		n, err := bs.Write(buf[:])
		if err != nil {
			panic(err)
		}
		wTotal += n
		if wTotal >= fileLength {
			break
		}
	}
	log.Printf("writeStream  write bytes=%v\n", wTotal)
}

func readStream(s net.Conn, fileLength int, wg *sync.WaitGroup) {
	defer wg.Done()
	buf := make([]byte, PACKAGE_SIZE)
	var rTotal int = 0
	bs:=bufio.NewReaderSize(s,PACKAGE_SIZE);
	for {
		n, err := bs.Read(buf)
		if err != nil && err != io.EOF {
			panic(err)
		}
		rTotal += n
		if n == 0 {
			break
		}
		if rTotal >= fileLength {
			break
		}
	}
	log.Printf("readStream read bytes=%v  ,  total length=%v\n", rTotal, fileLength)
}