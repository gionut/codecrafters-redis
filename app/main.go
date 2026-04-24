package main

import (
	"fmt"
	"net"
	"os"
	"bufio"
	"strings"
	"strconv"
	"time"
)

var _ = net.Listen
var _ = os.Exit

type Entry struct {
    Value  string
    Expiry time.Time // zero value means no expiry
}

func (e Entry) IsExpired() bool {
    return !e.Expiry.IsZero() && time.Now().After(e.Expiry)
}

// handleCommand dispatches a parsed command and writes the response to conn.
func handleCommand(conn net.Conn, cmd string, args []string, store map[string]Entry) {
	switch cmd {
		case "PING":
			conn.Write([]byte("+PONG\r\n"))
		case "ECHO":
			if len(args) != 1 {
				conn.Write([]byte("-ERR wrong number of arguments for 'echo' command\r\n"))
				return
			}
			conn.Write([]byte(bulkString(args[0])))
		case "SET":
			if len(args) < 2 {
				conn.Write([]byte("-ERR wrong number of arguments for 'set' command\r\n"))
				return
			}
			
			key, val := args[0], args[1]
			entry := Entry{Value: val}
			if len(args) == 4 && args[2] == "PX" {
				ms, err := strconv.Atoi(args[3])
				if err != nil {
					conn.Write([]byte("-ERR invalid expire time in 'set' command\r\n"))
					return
				}
				entry.Expiry = time.Now().Add(time.Duration(ms) * time.Millisecond)
			}
			store[key] = entry
			conn.Write([]byte("+OK\r\n"))
		case "GET":
			if len(args) != 1 {
				conn.Write([]byte("-ERR wrong number of arguments for 'get' command\r\n"))
				return
			}
			key := args[0]
			entry, exists := store[key]
			if exists && !entry.IsExpired() {
        		conn.Write([]byte(bulkString(entry.Value)))
    		} else {
        		if exists {
            		delete(store, key) // lazy expiry cleanup
        		}
        		conn.Write([]byte(bulkStringNull()))
    		}
			
		default:
			conn.Write([]byte("-ERR unknown command '" + cmd + "'\r\n"))
	}
}

func HandleConnection(conn net.Conn) {
	defer conn.Close()
	reader := bufio.NewReader(conn)
	store := make(map[string]Entry)
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			return
		}
		line = strings.TrimRight(line, "\r\n")

		if !strings.HasPrefix(line, "*") {
			conn.Write([]byte("-ERR invalid command. Must start with *\r\n"))
			continue
		}

		argn, err := strconv.Atoi(line[1:])
		if err != nil {
			conn.Write([]byte("-ERR invalid multibulk length\r\n"))
			continue
		}

		cmd, args, err := parseCommand(reader, argn)
		if err != nil {
			fmt.Println("Parse error:", err)
			return // connection is likely corrupted, bail out
		}

		handleCommand(conn, cmd, args, store)
	}
}

func main() {
	
	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}
	defer l.Close()

	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			continue
		}
		go HandleConnection(conn)
	}
}
