package main

import (
	"flag"
	"fmt"
	"strconv"
	"os"
	"os/signal"
	"syscall"
	"github.com/sci4me/yakvs/yakvs"
)

func main() {
	flag.Parse()

	if flag.NArg() != 1 {
		fmt.Println("Usage: yakvs <port>")
		return
	}

	sPort := flag.Arg(0)

	port, err := strconv.Atoi(sPort)
	if err != nil {
		fmt.Println("error parsing port:", err, "\nUsage: yakvs <port>")
		return
	}

	server := yakvs.NewServer()
	go server.Start(port)

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	signal.Notify(c, syscall.SIGTERM)
	<-c

	server.Stop()
}