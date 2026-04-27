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

func TestHandleSet(t *testing.T) {
	conn, cleanup := setupTestConnection(t)
	defer cleanup()
	
	_, err := conn.Write([]byte("*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n"))
	if err != nil {
		t.Fatal(err)
	}
	buffer, totalRead := readWithDeadline(t, conn, 10)

    expected := "+OK\r\n"
    assert.Equal(t, expected, string(buffer[:totalRead]), "Received data should match expected count")
}

func TestHandleGetSet(t *testing.T) {
	conn, cleanup := setupTestConnection(t)
	defer cleanup()
	
	_, err := conn.Write([]byte("*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n"))
	if err != nil {
		t.Fatal(err)
	}
	buffer, totalRead := readWithDeadline(t, conn, 10)

    expected := "+OK\r\n"
    assert.Equal(t, expected, string(buffer[:totalRead]), "Received data should match expected count")

	_, err = conn.Write([]byte("*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n"))
	if err != nil {
		t.Fatal(err)
	}
	buffer, totalRead = readWithDeadline(t, conn, 10)
	expected = bulkString("bar")
    assert.Equal(t, expected, string(buffer[:totalRead]), "Received data should match expected count")
}

func TestHandleGetNull(t *testing.T) {
	conn, cleanup := setupTestConnection(t)
	defer cleanup()
	
	_, err := conn.Write([]byte("*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n"))
	if err != nil {
		t.Fatal(err)
	}
	buffer, totalRead := readWithDeadline(t, conn, 10)

    expected := bulkStringNull()
    assert.Equal(t, expected, string(buffer[:totalRead]), "Received data should match expected count")
}

func TestHandleGetSetExpiry(t *testing.T) {
	conn, cleanup := setupTestConnection(t)
	defer cleanup()
	
	_, err := conn.Write([]byte("*5\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n$2\r\nPX\r\n$3\r\n100\r\n"))
	if err != nil {
		t.Fatal(err)
	}
	buffer, totalRead := readWithDeadline(t, conn, 10)

    expected := "+OK\r\n"
    assert.Equal(t, expected, string(buffer[:totalRead]), "Received data should match expected count")

	_, err = conn.Write([]byte("*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n"))
	if err != nil {
		t.Fatal(err)
	}
	buffer, totalRead = readWithDeadline(t, conn, 10)
	expected = bulkString("bar")
    assert.Equal(t, expected, string(buffer[:totalRead]), "Received data should match expected count")

	time.Sleep(200*time.Millisecond)
	_, err = conn.Write([]byte("*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n"))
	if err != nil {
		t.Fatal(err)
	}
	buffer, totalRead = readWithDeadline(t, conn, 10)
	expected = bulkStringNull()
    assert.Equal(t, expected, string(buffer[:totalRead]), "Received data should match expected count")
}

func TestHandleRpushListCreation(t *testing.T) {
	conn, cleanup := setupTestConnection(t)
	defer cleanup()
	
	_, err := conn.Write([]byte("*3\r\n$4\r\nRPUSH\r\n$5\r\nlist\r\n$3\r\nfoo\r\n"))
	if err != nil {
		t.Fatal(err)
	}
	buffer, totalRead := readWithDeadline(t, conn, 10)

    expected := respInteger(1)
    assert.Equal(t, expected, string(buffer[:totalRead]), "Received data should match expected count")
}

func TestHandleLpushListCreation(t *testing.T) {
	conn, cleanup := setupTestConnection(t)
	defer cleanup()
	
	_, err := conn.Write([]byte("*3\r\n$4\r\nLPUSH\r\n$5\r\nlist\r\n$3\r\nfoo\r\n"))
	if err != nil {
		t.Fatal(err)
	}
	buffer, totalRead := readWithDeadline(t, conn, 10)

    expected := respInteger(1)
    assert.Equal(t, expected, string(buffer[:totalRead]), "Received data should match expected count")
}

func TestHandleRpushListAppend(t *testing.T) {
	conn, cleanup := setupTestConnection(t)
	defer cleanup()
	
	_, err := conn.Write([]byte("*3\r\n$4\r\nRPUSH\r\n$5\r\nlist\r\n$1\r\na\r\n"))
	if err != nil {
		t.Fatal(err)
	}
	buffer, totalRead := readWithDeadline(t, conn, 10)

    expected := respInteger(1)
    assert.Equal(t, expected, string(buffer[:totalRead]), "Received data should match expected count")

	_, err = conn.Write([]byte("*5\r\n$4\r\nRPUSH\r\n$5\r\nlist\r\n$1\r\nb\r\n$1\r\nc\r\n$1\r\nd\r\n"))
	if err != nil {
		t.Fatal(err)
	}
	buffer, totalRead = readWithDeadline(t, conn, 10)

    expected = respInteger(4)
    assert.Equal(t, expected, string(buffer[:totalRead]), "Received data should match expected count")

	_, err = conn.Write([]byte("*4\r\n$4\r\nLRANGE\r\n$5\r\nlist\r\n$1\r\n0\r\n$1\r\n3\r\n"))
	if err != nil {
		t.Fatal(err)
	}
	buffer, totalRead = readWithDeadline(t, conn, 10)

	expected = respArray([]string{"a", "b", "c", "d"})
    assert.Equal(t, expected, string(buffer[:totalRead]), "Received data should match expected count")
}

func TestHandleLpushListAppend(t *testing.T) {
	conn, cleanup := setupTestConnection(t)
	defer cleanup()
	
	_, err := conn.Write([]byte("*3\r\n$4\r\nLPUSH\r\n$5\r\nlist\r\n$1\r\na\r\n"))
	if err != nil {
		t.Fatal(err)
	}
	buffer, totalRead := readWithDeadline(t, conn, 10)

    expected := respInteger(1)
    assert.Equal(t, expected, string(buffer[:totalRead]), "Received data should match expected count")

	_, err = conn.Write([]byte("*5\r\n$4\r\nLPUSH\r\n$5\r\nlist\r\n$1\r\nb\r\n$1\r\nc\r\n$1\r\nd\r\n"))
	if err != nil {
		t.Fatal(err)
	}
	buffer, totalRead = readWithDeadline(t, conn, 10)

    expected = respInteger(4)
    assert.Equal(t, expected, string(buffer[:totalRead]), "Received data should match expected count")

	_, err = conn.Write([]byte("*4\r\n$4\r\nLRANGE\r\n$5\r\nlist\r\n$1\r\n0\r\n$1\r\n3\r\n"))
	if err != nil {
		t.Fatal(err)
	}
	buffer, totalRead = readWithDeadline(t, conn, 10)

	expected = respArray([]string{"d", "c", "b", "a"})
    assert.Equal(t, expected, string(buffer[:totalRead]), "Received data should match expected count")
}

func TestHandleListRangeEmptyList(t *testing.T) {
	conn, cleanup := setupTestConnection(t)
	defer cleanup()
	
	_, err := conn.Write([]byte("*4\r\n$4\r\nLRANGE\r\n$5\r\nlist\r\n$1\r\n0\r\n$1\r\n1\r\n"))
	if err != nil {
		t.Fatal(err)
	}
	buffer, totalRead := readWithDeadline(t, conn, 10)

    expected := respArray([]string{})
    assert.Equal(t, expected, string(buffer[:totalRead]), "Received data should match expected count")
}

func TestHandleListRangeGoodIndices(t *testing.T) {
	conn, cleanup := setupTestConnection(t)
	defer cleanup()

	_, err := conn.Write([]byte("*4\r\n$4\r\nRPUSH\r\n$5\r\nlist\r\n$1\r\na\r\n$1\r\nb\r\n"))
	if err != nil {
		t.Fatal(err)
	}
	buffer, totalRead := readWithDeadline(t, conn, 10)

    expected := respInteger(2)
    assert.Equal(t, expected, string(buffer[:totalRead]), "Received data should match expected count")

	_, err = conn.Write([]byte("*4\r\n$4\r\nLRANGE\r\n$5\r\nlist\r\n$1\r\n0\r\n$1\r\n1\r\n"))
	if err != nil {
		t.Fatal(err)
	}
	buffer, totalRead = readWithDeadline(t, conn, 10)

	expected = respArray([]string{"a", "b"})
    assert.Equal(t, expected, string(buffer[:totalRead]), "Received data should match expected count")
}

func TestHandleListRangeExceedingStop(t *testing.T) {
	conn, cleanup := setupTestConnection(t)
	defer cleanup()

	_, err := conn.Write([]byte("*4\r\n$4\r\nRPUSH\r\n$5\r\nlist\r\n$1\r\na\r\n$1\r\nb\r\n"))
	if err != nil {
		t.Fatal(err)
	}
	buffer, totalRead := readWithDeadline(t, conn, 10)

    expected := respInteger(2)
    assert.Equal(t, expected, string(buffer[:totalRead]), "Received data should match expected count")

	_, err = conn.Write([]byte("*4\r\n$4\r\nLRANGE\r\n$5\r\nlist\r\n$1\r\n0\r\n$3\r\n100\r\n"))
	if err != nil {
		t.Fatal(err)
	}
	buffer, totalRead = readWithDeadline(t, conn, 10)
	
	expected = respArray([]string{"a", "b"})
    assert.Equal(t, expected, string(buffer[:totalRead]), "Received data should match expected count")
}

func TestHandleListRangeStartExceedingLength(t *testing.T) {
	conn, cleanup := setupTestConnection(t)
	defer cleanup()

	_, err := conn.Write([]byte("*4\r\n$4\r\nRPUSH\r\n$5\r\nlist\r\n$1\r\na\r\n$1\r\nb\r\n"))
	if err != nil {
		t.Fatal(err)
	}
	buffer, totalRead := readWithDeadline(t, conn, 10)

    expected := respInteger(2)
    assert.Equal(t, expected, string(buffer[:totalRead]), "Received data should match expected count")

	_, err = conn.Write([]byte("*4\r\n$4\r\nLRANGE\r\n$5\r\nlist\r\n$1\r\n2\r\n$3\r\n100\r\n"))
	if err != nil {
		t.Fatal(err)
	}
	buffer, totalRead = readWithDeadline(t, conn, 10)
	
	expected = respArray([]string{})
    assert.Equal(t, expected, string(buffer[:totalRead]), "Received data should match expected count")

}

func TestHandleListRangeStartExceedingStop(t *testing.T) {
	conn, cleanup := setupTestConnection(t)
	defer cleanup()
	
	_, err := conn.Write([]byte("*4\r\n$4\r\nRPUSH\r\n$5\r\nlist\r\n$1\r\na\r\n$1\r\nb\r\n"))
	if err != nil {
		t.Fatal(err)
	}
	buffer, totalRead := readWithDeadline(t, conn, 10)

    expected := respInteger(2)
    assert.Equal(t, expected, string(buffer[:totalRead]), "Received data should match expected count")

	_, err = conn.Write([]byte("*4\r\n$4\r\nLRANGE\r\n$5\r\nlist\r\n$1\r\n2\r\n$1\r\n1\r\n"))
	if err != nil {
		t.Fatal(err)
	}
	buffer, totalRead = readWithDeadline(t, conn, 10)
	
	expected = respArray([]string{})
    assert.Equal(t, expected, string(buffer[:totalRead]), "Received data should match expected count")
}

func TestHandleLlen(t *testing.T) {
	conn, cleanup := setupTestConnection(t)
	defer cleanup()
	
	_, err := conn.Write([]byte("*2\r\n$4\r\nLLEN\r\n$5\r\nlist\r\n"))
	if err != nil {
		t.Fatal(err)
	}
	buffer, totalRead := readWithDeadline(t, conn, 20)

    expected := respInteger(0)
    assert.Equal(t, expected, string(buffer[:totalRead]), "Received data should match expected count")

	_, err = conn.Write([]byte("*4\r\n$4\r\nRPUSH\r\n$5\r\nlist\r\n$1\r\na\r\n$1\r\nb\r\n"))
	if err != nil {
		t.Fatal(err)
	}
	buffer, totalRead = readWithDeadline(t, conn, 10)

    expected = respInteger(2)
    assert.Equal(t, expected, string(buffer[:totalRead]), "Received data should match expected count")

	_, err = conn.Write([]byte("*2\r\n$5\r\nLLEN\r\n$5\r\nlist\r\n"))
	if err != nil {
		t.Fatal(err)
	}
	buffer, totalRead = readWithDeadline(t, conn, 10)

    expected = respInteger(2)
    assert.Equal(t, expected, string(buffer[:totalRead]), "Received data should match expected count")

}