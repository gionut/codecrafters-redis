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

func HandleConnection(conn net.Conn) {
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
