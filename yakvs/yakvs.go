package yakvs

import (
	"bytes"
	"net"
	"sync"
	"strconv"
	"log"
	"os"
	"github.com/timtadh/netutils"
)

type Server struct {
	listener *net.TCPListener
	data map[string]string
	lock *sync.RWMutex
	logger *log.Logger
}

type connection struct {
	s *Server
	send chan<- []byte
	recv <-chan byte
}

func NewServer() *Server {
	s := new(Server)
	s.data = make(map[string]string)
	s.lock = new(sync.RWMutex)
	s.logger = log.New(os.Stdout, "yakvs> ", log.Ldate | log.Ltime)
	return s
}

func (s *Server) Start(port int) {
	listener, err := net.ListenTCP("tcp", &net.TCPAddr{IP: net.ParseIP("0.0.0.0"), Port: port})
	if err != nil {
		panic(err)
	}
	s.listener = listener
	s.logger.Println("started")
	s.listen()
}

func (s *Server) Stop() {
	s.listener.Close()
	s.logger.Println("stopped")
}

func (s *Server) Put(key, value string) {
	s.lock.Lock()
	defer s.lock.Unlock()

	s.logger.Println("put " + key + "=" + value)

	s.data[key] = value	
}

func (s *Server) Get(key string) (value string, has bool) {
	s.lock.RLock()
	defer s.lock.RUnlock()

	s.logger.Println("get", key)

	value, has = s.data[key]
	return
}

func (s *Server) Remove(key string) {
	s.lock.Lock()
	defer s.lock.Unlock()

	s.logger.Println("remove", key)

	delete(s.data, key)
}

func (s *Server) Clear() {
	s.lock.Lock()
	defer s.lock.Unlock()

	s.logger.Println("clear")

	s.data = make(map[string]string)
}

func (s *Server) List() (keys []string, values []string) {
	s.lock.RLock()
	defer s.lock.RUnlock()

	keys = make([]string, 0)
	values = make([]string, 0)

	for key, value := range s.data {
		keys = append(keys, key)
		values = append(values, value)
	}
	return
}	

func (s *Server) Size() int {
	s.lock.RLock()
	defer s.lock.RUnlock()

	return len(s.data)
}

func (s *Server) listen() {
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

func (s *Server) acceptConnection(send chan<- []byte, recv <-chan byte) *connection {
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

		if len(bSplit) < 1 {
			c.send <- []byte("ERROR\n")
		} else {
			split := make([]string, 0)
			for _, b := range bSplit {
				split = append(split, string(bytes.TrimSpace(b)))
			}

			switch split[0] {
			case "PUT":
				if len(split) != 3 {
					c.send <- []byte("ERROR\n")
				} else {
					c.s.Put(split[1], split[2])
					c.send <- []byte("OK\n")
				}
			case "GET":
				if len(split) != 2 {
					c.send <- []byte("ERROR\n")
				} else {
					value, has := c.s.Get(split[1])
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
					_, has := c.s.Get(split[1])
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
					c.s.Remove(split[1])
					c.send <- []byte("OK\n")
				}
			case "SIZE":
				if len(split) != 1 {
					c.send <- []byte("ERROR\n")
				} else {
					c.send <- []byte(strconv.Itoa(c.s.Size()) + "\n")
				}
			case "CLEAR":
				if len(split) != 1 {
					c.send <- []byte("ERROR\n")
				} else {
					c.s.Clear()
					c.send <- []byte("OK\n")
				}
			case "LIST":
				if len(split) == 1 || len(split) == 2 { 
					keys, values := c.s.List()	
					size := c.s.Size()

					if size == 0 {
						c.send <- []byte("nil\n")
					} else {
						var buf bytes.Buffer

						if len(split) == 1 {
							for i := 0; i < size; i++ {
								buf.WriteString(keys[i] + "=" + values[i] + "\n")
							}
						} else {
							switch split[1] {
							case "KEYS":
								for i := 0; i < size; i++ {
									buf.WriteString(keys[i] + "\n")
								}
							case "VALUES":
								for i := 0; i < size; i++ {
								buf.WriteString(values[i] + "\n")
							}
							default:
								buf.WriteString("ERROR\n")
							}
						}

						c.send <- buf.Bytes()
					}
				} else {
					c.send <- []byte("ERROR\n")
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
}

func (c *connection) close() {
	close(c.send)
	<-c.recv
	c.s.logger.Println("connection closed")
}	