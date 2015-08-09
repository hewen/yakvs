// Package yakvs provides a network-based, in-memory, key-value store.
package yakvs

import (
	"bytes"
	"github.com/timtadh/netutils"
	"log"
	"net"
	"os"
	"strconv"
	"sync"
)

const (
	MIN_VERBOSITY = 0
	MAX_VERBOSITY = 4

	VERBOSITY_CONNECTION_ACCEPTED = 0
	VERBOSITY_CONNECTION_CLOSED   = 0
	VERBOSITY_CONNECTION_REFUSED  = 0
	VERBOSITY_INVALID_COMMAND     = 0

	VERBOSITY_CLEAR = 1

	VERBOSITY_PUT    = 2
	VERBOSITY_REMOVE = 2

	VERBOSITY_GET      = 3
	VERBOSITY_HASKEY   = 3
	VERBOSITY_HASVALUE = 3

	VERBOSITY_LIST        = 4
	VERBOSITY_LIST_KEYS   = 4
	VERBOSITY_LIST_VALUES = 4
	VERBOSITY_SIZE        = 4
	VERBOSITY_QUIT        = 4
)

// Server represents a networked, in-memory, key-value store.
type Server struct {
	port     int
	listener *net.TCPListener

	data     map[string]string
	dataLock *sync.RWMutex

	logger *log.Logger

	connectionsLock *sync.RWMutex
	connections     map[uint64]*connection

	runningLock *sync.Mutex
	running     bool

	maxClients int
	verbosity  int
}

type connection struct {
	cid        uint64
	s          *Server
	send       chan<- []byte
	recv       <-chan byte
	closedLock *sync.Mutex
	closed     bool
}

// NewServer creates a new key-value store.
// port is the port the TCP listener will be started on.
// maxClients is the maximum number of active connections the server will allow.
// verbosity is whether the logging of the server will be verbosity or not.
func NewServer(port, maxClients int, verbosity int) *Server {
	s := new(Server)
	s.port = port
	s.data = make(map[string]string)
	s.dataLock = new(sync.RWMutex)
	s.logger = log.New(os.Stdout, "yakvs> ", log.Ldate|log.Ltime)
	s.connectionsLock = new(sync.RWMutex)
	s.connections = make(map[uint64]*connection)
	s.runningLock = new(sync.Mutex)
	s.maxClients = maxClients
	s.verbosity = verbosity
	return s
}

// Start starts the network listener on the store.
// This method blocks while the server is running.
func (s *Server) Start() {
	listener, err := net.ListenTCP("tcp", &net.TCPAddr{IP: net.ParseIP("0.0.0.0"), Port: s.port})
	if err != nil {
		panic(err)
	}
	s.listener = listener

	s.runningLock.Lock()
	s.running = true
	s.runningLock.Unlock()

	s.listen()
}

// Stop stops the server, closing all connections.
func (s *Server) Stop() {
	s.runningLock.Lock()
	s.running = false
	s.runningLock.Unlock()

	conns := make([]*connection, 0)

	s.connectionsLock.RLock()
	for _, conn := range s.connections {
		conns = append(conns, conn)
	}
	s.connectionsLock.RUnlock()

	for i := 0; i < len(conns); i++ {
		conn := conns[i]

		conn.closedLock.Lock()
		closed := conn.closed
		conn.closedLock.Unlock()
		if !closed {
			conn.send <- []byte("SERVER STOPPED\n")
			conn.close()
		}
	}

	s.listener.Close()
	s.logger.Println("stopped")
}

// Put adds a mapping from the specified value to the specified key.
func (s *Server) Put(key, value string) {
	s.dataLock.Lock()
	defer s.dataLock.Unlock()

	s.data[key] = value
}

// Get returns the value mapped to the specified key, or nil, as well as true if the mapping exists.
func (s *Server) Get(key string) (value string, has bool) {
	s.dataLock.RLock()
	defer s.dataLock.RUnlock()

	value, has = s.data[key]
	return
}

// HasKey returns true if the specified key has a mapping in this store.
func (s *Server) HasKey(key string) bool {
	_, has := s.Get(key)
	return has
}

// HasValue returns true if the specified value has a mapping in this store.
func (s *Server) HasValue(value string) bool {
	s.dataLock.RLock()
	defer s.dataLock.RUnlock()

	for _, v := range s.data {
		if v == value {
			return true
		}
	}

	return false
}

// Remove deletes a key-value mapping from this store.
// If the specified key does not have a mapping, nothing happens.
func (s *Server) Remove(key string) {
	s.dataLock.Lock()
	defer s.dataLock.Unlock()

	delete(s.data, key)
}

// Clear removes all key-value mappings from this store.
func (s *Server) Clear() {
	s.dataLock.Lock()
	defer s.dataLock.Unlock()

	s.data = make(map[string]string)
}

// List returns a slice containing all keys in this store, a slice containing all values in this store, and the size of the store.
func (s *Server) List() (keys []string, values []string, size int) {
	s.dataLock.RLock()
	defer s.dataLock.RUnlock()

	keys = make([]string, 0)
	values = make([]string, 0)

	for key, value := range s.data {
		keys = append(keys, key)
		values = append(values, value)
	}
	size = len(s.data)
	return
}

// Size returns the number of key-value mappings in this store.
func (s *Server) Size() int {
	s.dataLock.RLock()
	defer s.dataLock.RUnlock()

	return len(s.data)
}

func (s *Server) listen() {
	errors := make(chan error)
	go func() {
		for err := range errors {
			s.logger.Println(err)
		}
	}()

	s.logger.Println("yakvs server started")

	var cid uint64
	var eof bool
	for !eof {
		conn, err := s.listener.AcceptTCP()
		if netutils.IsEOF(err) {
			eof = true
		} else if err != nil {
			s.logger.Panic(err)
		} else {
			s.runningLock.Lock()
			isRunning := s.running
			s.runningLock.Unlock()

			if !isRunning {
				return
			}

			s.connectionsLock.RLock()
			activeConnections := len(s.connections)
			s.connectionsLock.RUnlock()

			if activeConnections < s.maxClients {
				send := netutils.TCPWriter(conn, errors)
				recv := netutils.TCPReader(conn, errors)
				go s.acceptConnection(cid, send, recv).serve()

				if s.verbosity >= VERBOSITY_CONNECTION_ACCEPTED {
					s.logger.Println("accepted connection " + strconv.FormatUint(cid, 10) + "@" + conn.RemoteAddr().String())
				}

				cid++
			} else {
				conn.Write([]byte("CONNECTION REFUSED\n"))
				conn.Close()

				if s.verbosity >= VERBOSITY_CONNECTION_REFUSED {
					s.logger.Println("ignored connection from " + conn.RemoteAddr().String())
				}
			}
		}
	}
}

func (s *Server) acceptConnection(cid uint64, send chan<- []byte, recv <-chan byte) *connection {
	conn := &connection{
		cid:        cid,
		s:          s,
		send:       send,
		recv:       recv,
		closedLock: new(sync.Mutex),
	}

	s.connectionsLock.Lock()
	defer s.connectionsLock.Unlock()

	s.connections[cid] = conn

	return conn
}

func (c *connection) serve() {
	defer c.close()

	const (
		ERROR   = "ERROR\n"
		OK      = "OK\n"
		TRUE    = "TRUE\n"
		FALSE   = "FALSE\n"
		NIL     = "nil\n"
		BYE     = "BYE\n"
		WELCOME = "WELCOME\n"
	)

	c.send <- []byte(WELCOME)

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
					if c.s.verbosity >= VERBOSITY_PUT {
						c.s.logger.Println("(cid:" + strconv.FormatUint(c.cid, 10) + ") put " + split[1] + "=" + split[2])
					}
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
					if c.s.verbosity >= VERBOSITY_GET {
						c.s.logger.Println("(cid:"+strconv.FormatUint(c.cid, 10)+") get", split[1])
					}
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
					if c.s.verbosity >= VERBOSITY_HASKEY {
						c.s.logger.Println("(cid:"+strconv.FormatUint(c.cid, 10)+") haskey", split[1])
					}
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
					if c.s.verbosity >= VERBOSITY_HASVALUE {
						c.s.logger.Println("(cid:"+strconv.FormatUint(c.cid, 10)+") hasvalue", split[1])
					}
				}
			case "REMOVE":
				if len(split) != 2 {
					buf.WriteString(ERROR)
				} else {
					c.s.Remove(split[1])
					buf.WriteString(OK)
					if c.s.verbosity >= VERBOSITY_REMOVE {
						c.s.logger.Println("(cid:"+strconv.FormatUint(c.cid, 10)+") remove", split[1])
					}
				}
			case "SIZE":
				if len(split) != 1 {
					buf.WriteString(ERROR)
				} else {
					buf.WriteString(strconv.Itoa(c.s.Size()) + "\n")
				}
				if c.s.verbosity >= VERBOSITY_SIZE {
					c.s.logger.Println("(cid:" + strconv.FormatUint(c.cid, 10) + ") size")
				}
			case "CLEAR":
				if len(split) != 1 {
					buf.WriteString(ERROR)
				} else {
					c.s.Clear()
					buf.WriteString(OK)
					if c.s.verbosity >= VERBOSITY_CLEAR {
						c.s.logger.Println("(cid:" + strconv.FormatUint(c.cid, 10) + ") clear")
					}
				}
			case "LIST":
				if len(split) == 1 || len(split) == 2 {
					keys, values, size := c.s.List()

					if size == 0 {
						buf.WriteString(NIL)
					} else {
						buf.WriteString(strconv.Itoa(size) + "\n")
						if len(split) == 1 {
							for i := 0; i < size; i++ {
								buf.WriteString(keys[i] + "=" + values[i] + "\n")
							}
							if c.s.verbosity >= VERBOSITY_LIST {
								c.s.logger.Println("(cid:" + strconv.FormatUint(c.cid, 10) + ") list")
							}
						} else {
							switch split[1] {
							case "KEYS":
								for i := 0; i < size; i++ {
									buf.WriteString(keys[i] + "\n")
								}
								if c.s.verbosity >= VERBOSITY_LIST_KEYS {
									c.s.logger.Println("(cid:" + strconv.FormatUint(c.cid, 10) + ") list keys")
								}
							case "VALUES":
								for i := 0; i < size; i++ {
									buf.WriteString(values[i] + "\n")
								}
								if c.s.verbosity >= VERBOSITY_LIST_VALUES {
									c.s.logger.Println("(cid:" + strconv.FormatUint(c.cid, 10) + ") list values")
								}
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
					c.send <- buf.Bytes()
					return
				}
				if c.s.verbosity >= VERBOSITY_QUIT {
					c.s.logger.Println("(cid:" + strconv.FormatUint(c.cid, 10) + ") quit")
				}
			default:
				buf.WriteString(ERROR)
				if c.s.verbosity >= VERBOSITY_INVALID_COMMAND {
					c.s.logger.Println("(cid:" + strconv.FormatUint(c.cid, 10) + ") invalid command: " + string(line))
				}
			}

			c.closedLock.Lock()
			if !c.closed {
				c.send <- buf.Bytes()
			}
			c.closedLock.Unlock()
		}
	}
}

func (c *connection) close() {
	c.closedLock.Lock()
	defer c.closedLock.Unlock()

	if c.closed {
		return
	}
	c.closed = true

	c.s.connectionsLock.Lock()
	defer c.s.connectionsLock.Unlock()

	delete(c.s.connections, c.cid)

	close(c.send)
	<-c.recv
	if c.s.verbosity >= VERBOSITY_CONNECTION_CLOSED {
		c.s.logger.Println("connection", c.cid, "closed")
	}
}
