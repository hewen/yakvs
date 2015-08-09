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
	VERBOSITY   = yakvs.MIN_VERBOSITY
)

func main() {
	fMaxClients := flag.Int("maxclients", MAX_CLIENTS, "")
	fMaxProcs := flag.Int("maxprocs", runtime.NumCPU(), "")
	fVerbosity := flag.Int("verbosity", VERBOSITY, "")

	flag.Parse()

	if flag.NArg() != 1 {
		fmt.Println("Usage: yakvs <port>")
		fmt.Println("Options:")
		fmt.Println("  -maxclients=<number>")
		fmt.Println("  -maxprocs=<number>")
		fmt.Println("  -verbosity=<number>")
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
	verbosity := *fVerbosity

	if maxClients < 1 {
		fmt.Println("maxclients must be > 0, using default")
		maxClients = MAX_CLIENTS
	}

	if maxProcs < 1 {
		fmt.Println("maxprocs must be > 0, using default")
		maxProcs = runtime.NumCPU()
	}

	if verbosity < yakvs.MIN_VERBOSITY || verbosity > yakvs.MAX_VERBOSITY {
		fmt.Println("verbosity must be > 0 and < ", yakvs.MAX_VERBOSITY + 1, "using default")
		verbosity = VERBOSITY
	}

	runtime.GOMAXPROCS(maxProcs)

	server := yakvs.NewServer(port, maxClients, verbosity)
	go server.Start()

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	signal.Notify(c, syscall.SIGTERM)
	<-c

	server.Stop()
}
