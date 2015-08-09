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

	s.data[key] = value	
}

func (s *Server) Get(key string) (value string, has bool) {
	s.lock.RLock()
	defer s.lock.RUnlock()

	value, has = s.data[key]
	return
}

func (s *Server) HasKey(key string) bool {
	_, has := s.Get(key)
	return has
}

func (s *Server) HasValue(value string) bool {
	s.lock.RLock()
	defer s.lock.RUnlock()

	for _, v := range s.data {
		if v == value {
			return true
		}
	}

	return false
}

func (s *Server) Remove(key string) {
	s.lock.Lock()
	defer s.lock.Unlock()

	delete(s.data, key)
}

func (s *Server) Clear() {
	s.lock.Lock()
	defer s.lock.Unlock()

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

	const (
		ERROR = "ERROR\n"
		OK = "OK\n"
		TRUE = "TRUE\n"
		FALSE = "FALSE\n"
		NIL = "nil\n"
		BYE = "BYE\n"
	)

	for line := range netutils.Readlines(c.recv) {
		bSplit := bytes.SplitN(line, []byte(" "), -1)

		var buf bytes.Buffer

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
					buf.WriteString(ERROR)
				} else {
					c.s.Put(split[1], split[2])
					buf.WriteString(OK)
					c.s.logger.Println("put " + split[1] + "=" + split[2])
				}
			case "GET":
				if len(split) != 2 {
					buf.WriteString(ERROR)
				} else {
					value, has := c.s.Get(split[1])
					if has {
						buf.WriteString(value + "\n")
					} else {
						buf.WriteString(NIL)
					}
					c.s.logger.Println("get", split[1])
				}
			case "HASKEY":
				if len(split) != 2 {
					buf.WriteString(ERROR)
				} else {
					has := c.s.HasKey(split[1])
					if has {
						buf.WriteString(TRUE)
					} else {
						buf.WriteString(FALSE)
					}	
					c.s.logger.Println("haskey", split[1])
				}
			case "HASVALUE":
				if len(split) != 2 {
					buf.WriteString(ERROR)
				} else {
					has := c.s.HasValue(split[1])
					if has {
						buf.WriteString(TRUE)
					} else {
						buf.WriteString(FALSE)
					}
					c.s.logger.Println("hasvalue", split[1])
				}
			case "REMOVE":
				if len(split) != 2 {
					buf.WriteString(ERROR)
				} else {
					c.s.Remove(split[1])
					buf.WriteString(OK)
					c.s.logger.Println("remove", split[1])
				}
			case "SIZE":
				if len(split) != 1 {
					buf.WriteString(ERROR)
				} else {
					buf.WriteString(strconv.Itoa(c.s.Size()) + "\n")
				}
				c.s.logger.Println("size")
			case "CLEAR":
				if len(split) != 1 {
					buf.WriteString(ERROR)
				} else {
					c.s.Clear()
					buf.WriteString(OK)
					c.s.logger.Println("clear")
				}
			case "LIST":
				if len(split) == 1 || len(split) == 2 { 
					keys, values := c.s.List()	
					size := c.s.Size()

					if size == 0 {
						buf.WriteString(NIL)
					} else {
						if len(split) == 1 {
							for i := 0; i < size; i++ {
								buf.WriteString(keys[i] + "=" + values[i] + "\n")
							}
							c.s.logger.Println("list")
						} else {
							switch split[1] {
							case "KEYS":
								for i := 0; i < size; i++ {
									buf.WriteString(keys[i] + "\n")
								}
								c.s.logger.Println("list keys")
							case "VALUES":
								for i := 0; i < size; i++ {
									buf.WriteString(values[i] + "\n")
								}
								c.s.logger.Println("list values")
							default:
								buf.WriteString(ERROR)
							}
						}
					}
				} else {
					buf.WriteString(ERROR)
				}
			case "QUIT":
				if len(split) != 1 {
					buf.WriteString(ERROR)
				} else {
					buf.WriteString(BYE)
					return
				}
				c.s.logger.Println("quit")
			default:
				buf.WriteString(ERROR)
				c.s.logger.Println("invalid command")
			}

			c.send <- buf.Bytes()
		}
	}
}

func (c *connection) close() {
	close(c.send)
	<-c.recv
	c.s.logger.Println("connection closed")
}	