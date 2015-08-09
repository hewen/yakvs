package main

import (
	"flag"
	"fmt"
	"github.com/sci4me/yakvs/yakvs"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"syscall"
)

const (
	MAX_CLIENTS = 10000
)

func main() {
	fMaxClients := flag.Int("maxclients", MAX_CLIENTS, "")
	fMaxProcs := flag.Int("maxprocs", runtime.NumCPU(), "")
	fVerbose := flag.Bool("verbose", false, "")

	flag.Parse()

	if flag.NArg() != 1 {
		fmt.Println("Usage: yakvs <port>")
		fmt.Println("Options:")
		fmt.Println("  -maxclients=<number>")
		fmt.Println("  -maxprocs=<number>")
		fmt.Println("  -verbose=<true|false>")
		return
	}

	sPort := flag.Arg(0)

	port, err := strconv.Atoi(sPort)
	if err != nil {
		fmt.Println("error parsing port:", err, "\nUsage: yakvs <port>")
		return
	}

	maxClients := *fMaxClients
	maxProcs := *fMaxProcs
	verbose := *fVerbose

	if maxClients < 1 {
		fmt.Println("maxclients must be > 0, using default")
		maxClients = MAX_CLIENTS
	}

	if maxProcs < 1 {
		fmt.Println("maxprocs must be > 0, using default")
		maxProcs = runtime.NumCPU()
	}

	runtime.GOMAXPROCS(maxProcs)

	server := yakvs.NewServer(port, maxClients, verbose)
	go server.Start()

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	signal.Notify(c, syscall.SIGTERM)
	<-c

	server.Stop()
}
