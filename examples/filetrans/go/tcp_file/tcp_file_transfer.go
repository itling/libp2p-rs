package main

import (
	"os"
	"context"
	"time"
	"net"
	"log"
	ra "math/rand"
	"io"
	"sync"
)

const PACKAGE_SIZE = 40 * 1024

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
		fileName, fileSize := generateRandFile(20000)
		s, err := net.Dial("tcp", "127.0.0.1:8080")
		if err != nil {
			panic(err)
		}
		start := time.Now()
		revFileName := fileName + "-rev"
		var wg sync.WaitGroup
		wg.Add(2)
		go readFileAndWriteStream(s, fileName,&wg)
		go readStreamAndWriteFile(s, revFileName,fileSize, &wg)
		wg.Wait()
		cost := time.Now().Sub(start).Seconds()
		log.Printf("readwrite cost time=%ds", int(cost))
		s.Close()
		err = os.Remove(revFileName)
		err = os.Remove(fileName)
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
		n, err := s.Read(buf)
		if err != nil {
			endTime := time.Now()
			sec := endTime.Sub(startTime).Seconds()
			rate := (rTotal / 1024/1024) / int(sec)
			log.Printf("total bytes=%d, cost=%ds, avg rate=%vm/s ", rTotal,int(sec), rate)

			cancelFunc()
			log.Printf(" read error:%s",  err)
			break
		}
		rTotal += n
		n, err = s.Write(buf[0:n])
		if err != nil {
			cancelFunc()
			log.Printf(" write error:%s", err)
			break
		}
		wTotal += n
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
				log.Printf("total bytes=%d,  rate=%vm/s ",*rTotal, 0)
			}else{
				rate := (*rTotal / 1024/1024) / int(sec)
				log.Printf("total bytes=%d,  rate=%vm/s ",*rTotal, rate)
			}
			lastRTotal=*rTotal
		}
	}
}



func readFileAndWriteStream(s  net.Conn, fileName string, wg *sync.WaitGroup) {
	defer wg.Done()
	f, err := os.Open(fileName)
	if err != nil {
		panic(err)
	}
	defer f.Close()
	var rTotal int = 0
	var wTotal int = 0
	buf := make([]byte, PACKAGE_SIZE)
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

func readStreamAndWriteFile(s net.Conn, revFileName string, fileLength int, wg *sync.WaitGroup) {
	defer wg.Done()
	f, err := os.Create(revFileName)
	if err != nil {
		panic(err)
	}
	defer f.Close()
	buf := make([]byte, PACKAGE_SIZE)
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
		data = append(data, 0x42)
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