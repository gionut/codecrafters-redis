package main

import (
	"bufio"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
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

func setupTestConnection(t * testing.T) (net.Conn, func()) {
	addr, ln_cleanup := setupTestServer(t)

    conn, err := net.Dial("tcp", addr)
    if err != nil {
		t.Fatal(err)
	}
    cleanup := func() {
		conn.Close()
		ln_cleanup()
	}

	return conn, cleanup
}

func readWithDeadline(t *testing.T, conn net.Conn, timeout int) ([]byte, int) {
	conn.SetReadDeadline(time.Now().Add(time.Duration(timeout) * time.Millisecond))

	buffer := make([]byte, 1024)
	totalRead := 0

	for {
		n, err := conn.Read(buffer[totalRead:])
		if n > 0 {
			totalRead += n
		}

		if err != nil {
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				break // Timeout is expected, stop reading
			}
			t.Fatal(err)
		}
	}
	return buffer, totalRead
}

func TestHandleConnection(t *testing.T) {
	conn, cleanup := setupTestConnection(t)
	defer cleanup()

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
    conn, cleanup := setupTestConnection(t)
	defer cleanup()

    for i := 0; i < 3; i++ {
        _, err := conn.Write([]byte("*1\r\n$4\r\nPING\r\n"))
        if err != nil {
			t.Fatal(err)
		}
    }

    buffer, totalRead := readWithDeadline(t, conn, 10)

    expected := "+PONG\r\n+PONG\r\n+PONG\r\n"
    assert.Equal(t, expected, string(buffer[:totalRead]), "Received data should match expected count")
}

func TestHandleEcho(t *testing.T) {
	conn, cleanup := setupTestConnection(t)
	defer cleanup()
	
	_, err := conn.Write([]byte("*2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n"))
	if err != nil {
		t.Fatal(err)
	}
	buffer, totalRead := readWithDeadline(t, conn, 10)

    expected := "$3\r\nhey\r\n"
    assert.Equal(t, expected, string(buffer[:totalRead]), "Received data should match expected count")
}