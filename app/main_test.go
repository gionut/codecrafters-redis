package main

import (
	"bufio"
	"net"
	"testing"
)

func setupTestServer(t *testing.T) (string, func()) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	go func() {
		conn, err := ln.Accept()
		if err == nil {
			HandleConnection(conn)
		}
	}()
	
	cleanup := func() {
        ln.Close()
    }
    
    return ln.Addr().String(), cleanup
	
}

func TestHandleConnection(t *testing.T) {
	ln_addr, cleanup := setupTestServer(t)
	defer cleanup()

	conn, err := net.Dial("tcp", ln_addr)
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

func TestHandleMultiplePings(t *testing.T) {
    ln_addr, cleanup := setupTestServer(t)
	defer cleanup()

	conn, err := net.Dial("tcp", ln_addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	for i := 0; i < 3; i++ {
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
}