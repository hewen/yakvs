package yakvs

import (
	"bytes"
	"strconv"
	"sync"
	"time"

	"github.com/timtadh/netutils"
)

type connection struct {
	cid uint64

	s *YAKVS

	send chan<- []byte
	recv <-chan byte

	closed     bool
	closedLock *sync.Mutex

	lastAccess     time.Time
	lastAccessLock *sync.Mutex
}

func (c *connection) writeString(s string) {
	c.send <- []byte(s)
}

func (c *connection) serve() {
	defer c.s.closeConnection(c)

	c.writeString(cWELCOME)

	for line := range netutils.Readlines(c.recv) {
		bSplit := bytes.SplitN(line, []byte(" "), -1)

		if len(bSplit) < 1 {
			c.writeString(cERROR)
		} else {
			split := make([]string, 0)
			for _, b := range bSplit {
				split = append(split, string(bytes.TrimSpace(b)))
			}

			c.lastAccessLock.Lock()
			c.lastAccess = time.Now()
			c.lastAccessLock.Unlock()

			switch split[0] {
			case cPUT:
				c.cPUT(split)
			case cGET:
				c.cGET(split)
			case cHASKEY:
				c.cHASKEY(split)
			case cHASVALUE:
				c.cHASVALUE(split)
			case cREMOVE:
				c.cREMOVE(split)
			case cSIZE:
				c.cSIZE(split)
			case cCLEAR:
				c.cCLEAR(split)
			case cLIST:
				c.cLIST(split)
			case cQUIT:
				c.cQUIT(split)
			default:
				c.writeString(cERROR)

				if c.s.config.Logging.Invalid_command {
					c.s.logger.Println("(cid:" + strconv.FormatUint(c.cid, 10) + ") invalid command: " + string(line))
				}
			}
		}
	}
}

func (c *connection) cPUT(split []string) {
	if len(split) != 3 {
		c.writeString(cERROR)
	} else {
		c.s.Put(split[1], split[2])
		c.writeString(cOK)

		if c.s.config.Logging.Put {
			c.s.logger.Println("(cid:" + strconv.FormatUint(c.cid, 10) + ") put " + split[1] + "=" + split[2])
		}
	}
}

func (c *connection) cGET(split []string) {
	if len(split) != 2 {
		c.writeString(cERROR)
	} else {
		value, has := c.s.Get(split[1])
		if has {
			c.writeString(value + "\n")
		} else {
			c.writeString(cNIL)
		}

		if c.s.config.Logging.Get {
			c.s.logger.Println("(cid:"+strconv.FormatUint(c.cid, 10)+") get", split[1])
		}
	}
}

func (c *connection) cHASKEY(split []string) {
	if len(split) != 2 {
		c.writeString(cERROR)
	} else {
		has := c.s.HasKey(split[1])
		if has {
			c.writeString(cTRUE)
		} else {
			c.writeString(cFALSE)
		}

		if c.s.config.Logging.Haskey {
			c.s.logger.Println("(cid:"+strconv.FormatUint(c.cid, 10)+") haskey", split[1])
		}
	}
}

func (c *connection) cHASVALUE(split []string) {
	if len(split) != 2 {
		c.writeString(cERROR)
	} else {
		has := c.s.HasValue(split[1])
		if has {
			c.writeString(cTRUE)
		} else {
			c.writeString(cFALSE)
		}

		if c.s.config.Logging.Haskey {
			c.s.logger.Println("(cid:"+strconv.FormatUint(c.cid, 10)+") hasvalue", split[1])
		}
	}
}

func (c *connection) cREMOVE(split []string) {
	if len(split) != 2 {
		c.writeString(cERROR)
	} else {
		c.s.Remove(split[1])
		c.writeString(cOK)

		if c.s.config.Logging.Remove {
			c.s.logger.Println("(cid:"+strconv.FormatUint(c.cid, 10)+") remove", split[1])
		}
	}
}

func (c *connection) cSIZE(split []string) {
	if len(split) != 1 {
		c.writeString(cERROR)
	} else {
		c.writeString(strconv.Itoa(c.s.Size()) + "\n")

		if c.s.config.Logging.Size {
			c.s.logger.Println("(cid:" + strconv.FormatUint(c.cid, 10) + ") size")
		}
	}
}

func (c *connection) cCLEAR(split []string) {
	if len(split) != 1 {
		c.writeString(cERROR)
	} else {
		c.s.Clear()
		c.writeString(cOK)

		if c.s.config.Logging.Clear {
			c.s.logger.Println("(cid:" + strconv.FormatUint(c.cid, 10) + ") clear")
		}
	}
}

func (c *connection) cLIST(split []string) {
	if len(split) == 1 || len(split) == 2 {
		keys, values, size := c.s.List()

		if size == 0 {
			c.writeString(cNIL)
		} else {
			var buf bytes.Buffer

			buf.WriteString(strconv.Itoa(size) + "\n")

			if len(split) == 1 {
				for i := 0; i < size; i++ {
					buf.WriteString(keys[i] + "=" + values[i] + "\n")
				}

				if c.s.config.Logging.List {
					c.s.logger.Println("(cid:" + strconv.FormatUint(c.cid, 10) + ") list")
				}
			} else {
				switch split[1] {
				case cKEYS:
					for i := 0; i < size; i++ {
						buf.WriteString(keys[i] + "\n")
					}

					if c.s.config.Logging.List_keys {
						c.s.logger.Println("(cid:" + strconv.FormatUint(c.cid, 10) + ") list keys")
					}
				case cVALUES:
					for i := 0; i < size; i++ {
						buf.WriteString(values[i] + "\n")
					}

					if c.s.config.Logging.List_keys {
						c.s.logger.Println("(cid:" + strconv.FormatUint(c.cid, 10) + ") list values")
					}
				default:
					buf.WriteString(cERROR)
				}
			}

			c.writeString(buf.String())
		}
	} else {
		c.writeString(cERROR)
	}
}

func (c *connection) cQUIT(split []string) {
	if len(split) != 1 {
		c.writeString(cERROR)
	} else {
		c.writeString(cBYE)

		if c.s.config.Logging.Quit {
			c.s.logger.Println("(cid:" + strconv.FormatUint(c.cid, 10) + ") quit")
		}

		return
	}
}
