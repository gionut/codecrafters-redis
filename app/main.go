package main

import (
	"fmt"
	"net"
	"os"
	"bufio"
	"strings"
	"strconv"
)

var _ = net.Listen
var _ = os.Exit


// handleCommand dispatches a parsed command and writes the response to conn.
func handleCommand(conn net.Conn, cmd string, args []string) {
	switch cmd {
		case "PING":
			conn.Write([]byte("+PONG\r\n"))
		case "ECHO":
			if len(args) != 1 {
				conn.Write([]byte("-ERR wrong number of arguments for 'echo' command\r\n"))
				return
			}
			conn.Write([]byte(bulkString(args[0])))
		default:
			conn.Write([]byte("-ERR unknown command '" + cmd + "'\r\n"))
	}
}

func HandleConnection(conn net.Conn) {
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

		handleCommand(conn, cmd, args)
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
