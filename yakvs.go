package main

import (
	"bytes"
	"net"
	"sync"
	"strconv"
	"log"
	"os"
	"flag"
	"fmt"
	"github.com/timtadh/netutils"
)

type server struct {
	listener *net.TCPListener
	data map[string]string
	lock *sync.RWMutex
	logger *log.Logger
}

func newServer() *server {
	s := new(server)
	s.data = make(map[string]string)
	s.lock = new(sync.RWMutex)
	s.logger = log.New(os.Stdout, "yakvs> ", log.Ldate | log.Ltime)
	return s
}

func (s *server) start(port int) {
	listener, err := net.ListenTCP("tcp", &net.TCPAddr{IP: net.ParseIP("0.0.0.0"), Port: port})
	if err != nil {
		panic(err)
	}
	s.listener = listener
	s.listen()
}

func (s *server) put(key, value string) {
	s.lock.Lock()
	defer s.lock.Unlock()

	s.logger.Println("put " + key + "=" + value)

	s.data[key] = value	
}

func (s *server) get(key string) (value string, has bool) {
	s.lock.RLock()
	defer s.lock.RUnlock()

	s.logger.Println("get", key)

	value, has = s.data[key]
	return
}

func (s *server) remove(key string) {
	s.lock.Lock()
	defer s.lock.Unlock()

	s.logger.Println("remove", key)

	delete(s.data, key)
}

func (s *server) size() int {
	s.lock.RLock()
	defer s.lock.RUnlock()

	return len(s.data)
}

func (s *server) listen() {
	errors := make(chan error)
	go func() {
		for err := range errors {
			s.logger.Println(err)
		}
	}()

	var eof bool
	for !eof {
		conn, err := s.listener.AcceptTCP()
		if netutils.IsEOF(err) {
			eof = true
		} else if err != nil {
			s.logger.Panic(err)
		} else {
			send := netutils.TCPWriter(conn, errors)
			recv := netutils.TCPReader(conn, errors)
			go s.acceptConnection(send, recv).serve()
		}
	}
}

type connection struct {
	s *server
	send chan<- []byte
	recv <-chan byte
}

func (s *server) acceptConnection(send chan<- []byte, recv <-chan byte) *connection {
	s.logger.Println("accepted connection")
	return &connection{
		s: s,
		send: send,
		recv: recv,
	}
}

func (c *connection) serve() {
	defer c.close()

	for line := range netutils.Readlines(c.recv) {
		bSplit := bytes.SplitN(line, []byte(" "), -1)

		split := make([]string, 0)
		for _, b := range bSplit {
			split = append(split, string(bytes.TrimSpace(b)))
		}

		if len(split) < 1 {
			c.send <- []byte("ERROR\n")
			return
		}

		switch split[0] {
		case "PUT":
			if len(split) != 3 {
				c.send <- []byte("ERROR\n")
			} else {
				c.s.put(split[1], split[2])
				c.send <- []byte("OK\n")
			}
		case "GET":
			if len(split) != 2 {
				c.send <- []byte("ERROR\n")
			} else {
				value, has := c.s.get(split[1])
				if has {
					c.send <- []byte(value + "\n")
				} else {
					c.send <- []byte("nil\n")
				}
			}
		case "HAS":
			if len(split) != 2 {
				c.send <- []byte("ERROR\n")
			} else {
				_, has := c.s.get(split[1])
				if has {
					c.send <- []byte("TRUE\n")
				} else {
					c.send <- []byte("FALSE\n")
				}	
			}
		case "REMOVE":
			if len(split) != 2 {
				c.send <- []byte("ERROR\n")
			} else {
				c.s.remove(split[1])
				c.send <- []byte("OK\n")
			}
		case "SIZE":
			if len(split) != 1 {
				c.send <- []byte("ERROR\n")
			} else {
				c.send <- []byte(strconv.Itoa(c.s.size()) + "\n")
			}
		case "QUIT":
			if len(split) != 1 {
				c.send <- []byte("ERROR\n")
			} else {
				c.send <- []byte("BYE\n")
				return
			}
		default:
			c.send <- []byte("ERROR\n")
		}
	}
}

func (c *connection) close() {
	close(c.send)
	<-c.recv
	c.s.logger.Println("connection closed")
}	

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

	server := newServer()
	server.start(port)
}