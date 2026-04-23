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


// parseRESP reads a single RESP value (length line + payload line) from the reader.
func parseRESP(reader *bufio.Reader) (string, error) {
	_, err := reader.ReadString('\n') // consume length line
	if err != nil {
		return "", fmt.Errorf("missing length: %w", err)
	}
	payload, err := reader.ReadString('\n')
	if err != nil {
		return "", fmt.Errorf("missing payload: %w", err)
	}
	return strings.TrimRight(payload, "\r\n"), nil
}


// parseCommand reads a full RESP array command from the reader,
// returning the command name and its arguments.
func parseCommand(reader *bufio.Reader, argn int) (string, []string, error) {
	cmd, err := parseRESP(reader)
	if err != nil {
		return "", nil, fmt.Errorf("parsing command: %w", err)
	}

	args := make([]string, 0, argn-1)
	for range argn - 1 {
		arg, err := parseRESP(reader)
		if err != nil {
			return "", nil, fmt.Errorf("parsing arg %d: %w", len(args)+1, err)
		}
		args = append(args, arg)
	}

	return cmd, args, nil
}

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
			conn.Write([]byte("+" + args[0] + "\r\n"))
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

func HandleConnectionOld(conn net.Conn) {
	defer conn.Close()
	reader := bufio.NewReader(conn)

	for {
		line, err := reader.ReadString('\n')
        if err != nil {
            return 
        }
		line = strings.Trim(line, "\r\n")

        if strings.HasPrefix(line, "*") {
			argn, err := strconv.Atoi(line[1:])
			if err != nil {
				continue
			}
			
			// Parse command
			_, err = reader.ReadString('\n')
			if err != nil {
				fmt.Println("Invalid request. Missing length")
				continue
			}
			cmd, err := reader.ReadString('\n')
			if err != nil {
				fmt.Println("Invalid request. Missing payload")
				continue
			}
			cmd = strings.Trim(cmd, "\r\n")

			var args []string
			for range(argn-1) {
				_, err = reader.ReadString('\n') // Consume lenght
				if err != nil {
					fmt.Println("Invalid request. Missing length")
					continue
				}
				arg, err := reader.ReadString('\n') // Consume payload
				if err != nil {
					fmt.Println("Invalid request. Missing arg")
					continue
				}
				args = append(args, strings.Trim(arg, "\r\n"))
			}

			if cmd == "PING" {
				conn.Write([]byte("+PONG\r\n"))
				continue
			}
			if cmd == "ECHO" {
				if len(args) != 1 {
					fmt.Println("Invalid request. Missing arg")
					continue
				}
				conn.Write([]byte(("+" + args[0] + "\r\n")))
				continue
			}
		}      
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
