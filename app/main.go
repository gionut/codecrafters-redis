package main

import (
	"fmt"
	"net"
	"os"
	"bufio"
	"strings"
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

        if strings.HasPrefix(line, "*") {
            _, err = reader.ReadString('\n') // Consume lenght
			if err != nil {
				fmt.Println("Invalid request. Missing length")
				continue
			}
            _, err = reader.ReadString('\n') // Consume payload
			if err != nil {
				fmt.Println("Invalid request. Missing payload")
				continue
			}
		}

		conn.Write([]byte("+PONG\r\n"))
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
