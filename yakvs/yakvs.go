package yakvs

import (
	"errors"
	"github.com/timtadh/netutils"
	"log"
	"net"
	"os"
	"strconv"
	"sync"
	"time"
)

// Config represents a set of configuration options for a YAKVS server.
type Config struct {
	Server struct {
		Port               int
		Max_clients        int
		Connection_timeout time.Duration
	}

	Logging struct {
		Connection_accepted bool
		Connection_closed   bool
		Connection_refused  bool
		Invalid_command     bool
		Clear               bool
		Put                 bool
		Remove              bool
		Get                 bool
		Haskey              bool
		Hasvalue            bool
		List                bool
		List_keys           bool
		List_values         bool
		Size                bool
		Quit                bool
	}
}

// YAKVS represents a networked, in-memory, key-value store.
type YAKVS struct {
	config Config

	listener *net.TCPListener

	data     map[string]string
	dataLock *sync.RWMutex

	connections     map[uint64]*connection
	connectionsLock *sync.RWMutex

	running     bool
	runningLock *sync.Mutex

	logger *log.Logger
}

// NewServer creates a new key-value store.
// config is the server configuration settings.
func NewServer(cfg Config) *YAKVS {
	s := new(YAKVS)

	s.config = cfg

	s.data = make(map[string]string)
	s.dataLock = new(sync.RWMutex)

	s.connections = make(map[uint64]*connection)
	s.connectionsLock = new(sync.RWMutex)

	s.runningLock = new(sync.Mutex)

	s.logger = log.New(os.Stdout, "yakvs> ", log.Ldate|log.Ltime)

	return s
}

// Start starts the network listener on the store.
// This method blocks while the server is running.
func (s *YAKVS) Start() error {
	s.runningLock.Lock()
	alreadyRunning := s.running

	if alreadyRunning {
		s.runningLock.Unlock()
		return errors.New("yakvs already started")
	}

	listener, err := net.ListenTCP("tcp", &net.TCPAddr{IP: net.ParseIP("0.0.0.0"), Port: s.config.Server.Port})
	if err != nil {
		return err
	}
	s.listener = listener

	s.running = true
	s.runningLock.Unlock()

	go s.listen()

	return nil
}

// Stop stops the server, closing all connections.
func (s *YAKVS) Stop() error {
	s.runningLock.Lock()
	defer s.runningLock.Unlock()

	if !s.running {
		return errors.New("yakvs server not running")
	}
	s.running = false

	conns := make([]*connection, 0)

	s.connectionsLock.RLock()
	for _, conn := range s.connections {
		conns = append(conns, conn)
	}
	s.connectionsLock.RUnlock()

	for i := 0; i < len(conns); i++ {
		conn := conns[i]

		conn.writeString("SERVER STOPPED\n")
		s.closeConnection(conn)
	}

	s.listener.Close()

	s.logger.Println("yakvs server stopped")

	return nil
}

// Put adds a mapping from the specified value to the specified key.
func (s *YAKVS) Put(key, value string) {
	s.dataLock.Lock()
	defer s.dataLock.Unlock()

	s.data[key] = value
}

// Get returns the value mapped to the specified key, or nil, as well as true if the mapping exists.
func (s *YAKVS) Get(key string) (value string, has bool) {
	s.dataLock.RLock()
	defer s.dataLock.RUnlock()

	value, has = s.data[key]
	return
}

// HasKey returns true if the specified key has a mapping in this store.
func (s *YAKVS) HasKey(key string) bool {
	_, has := s.Get(key)
	return has
}

// HasValue returns true if the specified value has a mapping in this store.
func (s *YAKVS) HasValue(value string) bool {
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
func (s *YAKVS) Remove(key string) {
	s.dataLock.Lock()
	defer s.dataLock.Unlock()

	delete(s.data, key)
}

// Clear removes all key-value mappings from this store.
func (s *YAKVS) Clear() {
	s.dataLock.Lock()
	defer s.dataLock.Unlock()

	s.data = make(map[string]string)
}

// List returns a slice containing all keys in this store, a slice containing all values in this store, and the size of the store.
func (s *YAKVS) List() (keys []string, values []string, size int) {
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
func (s *YAKVS) Size() int {
	s.dataLock.RLock()
	defer s.dataLock.RUnlock()

	return len(s.data)
}

func (s *YAKVS) listen() {
	errChan := make(chan error)
	go func() {
		for err := range errChan {
			s.logger.Println(err)
		}
	}()

	ticker := time.NewTicker(time.Millisecond * 100) // TODO config option?
	stopTickGoroutine := make(chan bool)
	go func() {
		for {
			select {
			case <-stopTickGoroutine:
				ticker.Stop()
				return
			case <-ticker.C:
				if s.config.Server.Connection_timeout > 0 {
					toTimeout := make([]*connection, 0)

					dTimeout := time.Second * s.config.Server.Connection_timeout

					s.connectionsLock.RLock()
					for _, conn := range s.connections {
						conn.lastAccessLock.Lock()
						since := time.Since(conn.lastAccess)
						conn.lastAccessLock.Unlock()

						if since >= dTimeout {
							toTimeout = append(toTimeout, conn)
						}
					}
					s.connectionsLock.RUnlock()
				
					for _, conn := range toTimeout {
						conn.writeString("TIMED OUT\n")
						s.closeConnection(conn)
					}
				}
			}
		}
	}()	

	s.logger.Println("yakvs server started")

	var cid uint64
	var exit bool
	for !exit {
		conn, err := s.listener.AcceptTCP()
		if netutils.IsEOF(err) {
			exit = true
		} else if err != nil {
			s.logger.Panic(err)
		} else {
			s.runningLock.Lock()
			isRunning := s.running
			s.runningLock.Unlock()

			var canConnect bool
			if isRunning {
				maxClients := s.config.Server.Max_clients

				if maxClients == 0 {
					canConnect = true
				} else {
					s.connectionsLock.Lock()
					canConnect = len(s.connections) < maxClients
					s.connectionsLock.Unlock()
				}
			} else {
				canConnect = false
				exit = true
			}

			if canConnect {
				go s.accept(cid, conn, errChan).serve()
				if s.config.Logging.Connection_accepted {
					s.logger.Println("accepted connection " + strconv.FormatUint(cid, 10) + "@" + conn.RemoteAddr().String())
				}
				cid++
			} else {
				conn.Write([]byte("CONNECTION REFUSED\n"))
				conn.Close()

				if s.config.Logging.Connection_refused {
					s.logger.Println("ignored connection from " + conn.RemoteAddr().String())
				}
			}
		}
	}

	close(errChan)
	stopTickGoroutine <- true
}

func (s *YAKVS) accept(cid uint64, conn *net.TCPConn, errChan chan error) *connection {
	c := new(connection)

	c.cid = cid
	c.s = s
	c.send = netutils.TCPWriter(conn, errChan)
	c.recv = netutils.TCPReader(conn, errChan)
	c.closedLock = new(sync.Mutex)
	c.lastAccessLock = new(sync.Mutex)

	c.lastAccessLock.Lock()
	c.lastAccess = time.Now()
	c.lastAccessLock.Unlock()

	s.connectionsLock.Lock()
	s.connections[cid] = c
	s.connectionsLock.Unlock()

	return c
}

func (s *YAKVS) closeConnection(c *connection) {
	c.closedLock.Lock()
	defer c.closedLock.Unlock()

	if c.closed {
		return
	}
	c.closed = true

	s.connectionsLock.Lock()
	defer s.connectionsLock.Unlock()

	delete(s.connections, c.cid)

	close(c.send)
	<-c.recv

	if s.config.Logging.Connection_closed {
		s.logger.Println("connection", c.cid, "closed")
	}
}