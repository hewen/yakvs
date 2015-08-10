package main

import (
	"code.google.com/p/gcfg"
	"fmt"
	"github.com/sci4me/yakvs/yakvs"
	"os"
	"os/signal"
	"syscall"
)

const defaultConfig = `[server]
port=2244
max-clients=10000
connection-timeout=0 # in seconds. 0 = disabled
max-procs=0 # 0 = runtime.NumCPU()

[logging]
connection-accepted=true
connection-closed=true
connection-refused=true
invalid-command=true
clear=true
put=true
remove=true
get=true
haskey=true
hasvalue=true
list=true
list-keys=true
list-values=true
size=true
quit=true
`

func loadConfig(filePath string) (*yakvs.Config, error) {
	cfg := new(yakvs.Config)

	file, err := os.Open(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			file, err = os.Create(filePath)
			if err != nil {
				return nil, err
			}

			file.WriteString(defaultConfig)
			file.WriteString("\n")
			file.Close()

			file, err = os.Open(filePath)
			if err != nil {
				return nil, err
			}
		} else {
			return nil, err
		}
	}
	defer file.Close()

	err = gcfg.ReadInto(cfg, file)
	if err != nil {
		return nil, err
	}

	return cfg, nil
}

func main() {
	pCfg, err := loadConfig("yakvs.conf")
	if err != nil {
		panic(err)
	}
	cfg := *pCfg

	if cfg.Server.Max_clients <= 0 {
		fmt.Println("Config error: max-clients cannot be less than one")
		return
	}

	if cfg.Server.Connection_timeout < 0 {
		fmt.Println("Config error: connection-timeout cannot be less than zero")
		return
	}

	// TODO GOMAXPROCS

	server := yakvs.NewServer(cfg)
	err = server.Start()
	if err != nil {
		panic(err)
	}

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	signal.Notify(c, syscall.SIGTERM)
	<-c

	err = server.Stop()
	if err != nil {
		panic(err)
	}
}
