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

// handleCommand dispatches a parsed command and writes the response to conn.
func handleCommand(conn net.Conn, cmd string, args []string, store *Store) {
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
			store.strings[key] = entry
			conn.Write([]byte("+OK\r\n"))
		case "GET":
			if len(args) != 1 {
				conn.Write([]byte("-ERR wrong number of arguments for 'get' command\r\n"))
				return
			}
			key := args[0]
			entry, exists := store.strings[key]
			if exists && !entry.IsExpired() {
        		conn.Write([]byte(bulkString(entry.Value)))
    		} else {
        		if exists {
            		delete(store.strings, key) // lazy expiry cleanup
        		}
        		conn.Write([]byte(bulkStringNull()))
    		}
		case "RPUSH":
			if len(args) < 2 {
				conn.Write([]byte("-ERR wrong number of arguments for 'rpush' command\r\n"))
				return
			}
			key := args[0]
			elements := args[1:]			
			conn.Write([]byte(respInteger(store.PushBack(key, elements))))

		case "LRANGE":
			if len(args) != 3 {
				conn.Write([]byte("-ERR wrong number of arguments for 'lrange' command\r\n"))
				return
			}
			key := args[0]
			start, err := strconv.Atoi(args[1])
			if err != nil {
				conn.Write([]byte("-ERR wrong value type for 'lrange <start>' argument\r\n"))
				return
			}
			stop, err := strconv.Atoi(args[2])
			if err != nil {
				conn.Write([]byte("-ERR wrong value type for 'lrange <stop>' argument\r\n"))
				return
			}

			conn.Write([]byte(respArray(store.SliceOfList(key, start, stop))))
		case "LPUSH":
			if len(args) < 2 {
				conn.Write([]byte("-ERR wrong number of arguments for 'lpush' command\r\n"))
				return
			}
			key := args[0]
			elements := args[1:]

			conn.Write([]byte(respInteger(store.PushFront(key, elements))))
		case "LLEN":
			if len(args) != 1 {
				conn.Write([]byte("-ERR wrong number of arguments for 'llist' command\r\n"))
				return
			}
			key := args[0]
			conn.Write([]byte(respInteger(store.ListLen(key))))

		case "LPOP":
			if len(args) > 2 {
				conn.Write([]byte("-ERR wrong number of arguments for 'lpop' command\r\n"))
				return
			}
			key := args[0]
			cnt := 1
			var err error
			if len(args) == 2 {
				cnt, err = strconv.Atoi(args[1])
				if err != nil {
					conn.Write([]byte("-ERR wrong type for 'lpop <count>' argument\r\n"))
					return
				}
			}

			result := store.Lpop(key, cnt)
			switch len(result) {
				case 0:
					conn.Write([]byte(bulkStringNull()))
				case 1: 
					conn.Write([]byte(bulkString(result[0])))
				default:
					conn.Write([]byte(respArray(result)))
			}
		case "BLPOP":
			if len(args) != 2 {
				conn.Write([]byte("-ERR wrong number of arguments for 'blpop' command\r\n"))
				return
			}
			key := args[0]
			timeout, err := strconv.Atoi(args[1])
			if err != nil || timeout != 0 {
				// For now, client will always call blpop with timeout 0
				conn.Write([]byte("-ERR wrong type for 'blpop <timeout>' argument\r\n"))
				return
			}
			
			val, popped := store.LPopOrRegister(key, Request{conn: conn, timeout: timeout})
			if popped {
				conn.Write([]byte(fmt.Sprintf("*2\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", len(key), key, len(val), val)))
			}
		default:
			conn.Write([]byte("-ERR unknown command '" + cmd + "'\r\n"))
	}
}

func HandleConnectionWithStore(conn net.Conn, store *Store) {
    defer conn.Close()
	reader := bufio.NewReader(conn)

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
	store := NewStore()

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
		go HandleConnectionWithStore(conn, store)
	}
}
