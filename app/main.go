package main

import (
	"fmt"
	"net"
	"os"
	"bufio"
	"strings"
	"strconv"
	"time"
	"container/list"
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
			
			l, exists := store.lists[key]
			if !exists {
				l = list.New()
				store.lists[key] = l
			}
			for _, el := range(elements) {
				l.PushBack(el)
			}
			
			conn.Write([]byte(respInteger(l.Len())))
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

			l, exists := store.lists[key]
			if !exists {
				conn.Write([]byte(respArray([]string{})))
				return
			}

			n := l.Len()
			if start < 0 {
				start = max(start+n, 0)
			}
			if stop < 0 {
				stop = stop + n
			}

			if start > stop || start >= n {
				conn.Write([]byte(respArray([]string{})))
				return
			}

			stop = min(stop+1, n)
			result := []string{}

			i := 0
			for e := l.Front(); e != nil; e = e.Next() {
				if i >= stop {
					break
				}
				if i >= start {
					result = append(result, e.Value.(string))
				}
				i++
			}
			conn.Write([]byte(respArray(result)))
		case "LPUSH":
			if len(args) < 2 {
				conn.Write([]byte("-ERR wrong number of arguments for 'lpush' command\r\n"))
				return
			}
			key := args[0]
			elements := args[1:]
			
			l, exists := store.lists[key]
			if !exists {
				l = list.New()
				store.lists[key] = l
			}
			for _, el := range(elements) {
				l.PushFront(el)
			}
			
			conn.Write([]byte(respInteger(l.Len())))
		case "LLEN":
			if len(args) != 1 {
				conn.Write([]byte("-ERR wrong number of arguments for 'llist' command\r\n"))
				return
			}
			key := args[0]
			
			l, exists := store.lists[key]
			if exists {
				conn.Write([]byte(respInteger(l.Len())))
				return
			}
			conn.Write([]byte(respInteger(0)))
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

			l, exists := store.lists[key]
			if !exists {
				conn.Write([]byte(bulkStringNull()))
				return
			}
			
			result := []string{}
			for e := l.Front(); e != nil && cnt > 0; {
				next := e.Next()
				result = append(result, l.Remove(e).(string))
				cnt--
				e = next
			}

			switch len(result) {
				case 0:
					conn.Write([]byte(bulkStringNull()))
				case 1: 
					conn.Write([]byte(bulkString(result[0])))
				default:
					conn.Write([]byte(respArray(result)))
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

func HandleConnection(conn net.Conn) {
	HandleConnectionWithStore(conn, NewStore())
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
