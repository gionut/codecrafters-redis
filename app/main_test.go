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

func TestHandleConnection(t *testing.T) {
	addr, cleanup := setupTestServer(t)
	defer cleanup()

	conn, err := net.Dial("tcp", addr)
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
    addr, cleanup := setupTestServer(t)
	defer cleanup()

    conn, err := net.Dial("tcp", addr)
    if err != nil {
		t.Fatal(err)
	}
    defer conn.Close()

    for i := 0; i < 3; i++ {
        _, err = conn.Write([]byte("*1\r\n$4\r\nPING\r\n"))
        if err != nil {
			t.Fatal(err)
		}
    }

    conn.SetReadDeadline(time.Now().Add(10 * time.Millisecond))

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

    expected := "+PONG\r\n+PONG\r\n+PONG\r\n"
    assert.Equal(t, expected, string(buffer[:totalRead]), "Received data should match expected count")
}