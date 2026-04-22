package main

import (
	"bufio"
	"net"
	"testing"
)

func TestHandleConnection(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		HandleConnection(conn)
	}()

	conn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	conn.Write([]byte("*1\r\n$4\r\nPING\r\n"))

	reader := bufio.NewReader(conn)
	resp, err := reader.ReadString('\n')
	if err != nil {
		t.Fatal(err)
	}

	expected := "+PONG\r\n"
	if resp != expected {
		t.Errorf("Expected %q, got %q", expected, resp)
	}
}