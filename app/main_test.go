package main

import (
	"bufio"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func setupTestServerWithStore(t *testing.T, store *Store) (string, func()) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	go func() {
		conn, err := ln.Accept()
		if err == nil {
			HandleConnectionWithStore(conn, store)
		}
	}()

	cleanup := func() {
		ln.Close()
	}

	return ln.Addr().String(), cleanup
}

func setupTestConnectionWithStore(t *testing.T, store *Store) (net.Conn, func()) {
	addr, ln_cleanup := setupTestServerWithStore(t, store)

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

func readWithDeadlineError(t *testing.T, conn net.Conn, timeout int) ([]byte, int, error) {
	conn.SetReadDeadline(time.Now().Add(time.Duration(timeout) * time.Millisecond))

	buffer := make([]byte, 1024)
	totalRead := 0
	var err error
	n := -1
	for {
		n, err = conn.Read(buffer[totalRead:])
		if n > 0 {
			totalRead += n
		}

		if err != nil {
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				break // Timeout is expected, stop reading
			}
		}
	}
	return buffer, totalRead, err
}

func TestHandleConnection(t *testing.T) {
	conn, cleanup := setupTestConnectionWithStore(t, NewStore())
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
	conn, cleanup := setupTestConnectionWithStore(t, NewStore())
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
	conn, cleanup := setupTestConnectionWithStore(t, NewStore())
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
	conn, cleanup := setupTestConnectionWithStore(t, NewStore())
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
	conn, cleanup := setupTestConnectionWithStore(t, NewStore())
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
	conn, cleanup := setupTestConnectionWithStore(t, NewStore())
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
	conn, cleanup := setupTestConnectionWithStore(t, NewStore())
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

	time.Sleep(200 * time.Millisecond)
	_, err = conn.Write([]byte("*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n"))
	if err != nil {
		t.Fatal(err)
	}
	buffer, totalRead = readWithDeadline(t, conn, 10)
	expected = bulkStringNull()
	assert.Equal(t, expected, string(buffer[:totalRead]), "Received data should match expected count")
}

func TestHandleRpushListCreation(t *testing.T) {
	conn, cleanup := setupTestConnectionWithStore(t, NewStore())
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
	conn, cleanup := setupTestConnectionWithStore(t, NewStore())
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
	store := StoreWithList("list", []string{"a"})
	conn, cleanup := setupTestConnectionWithStore(t, store)
	defer cleanup()

	_, err := conn.Write([]byte("*5\r\n$4\r\nRPUSH\r\n$5\r\nlist\r\n$1\r\nb\r\n$1\r\nc\r\n$1\r\nd\r\n"))
	if err != nil {
		t.Fatal(err)
	}
	buffer, totalRead := readWithDeadline(t, conn, 10)

	assert.Equal(t, respInteger(4), string(buffer[:totalRead]), "Received data should match expected count")
	assert.Equal(t, []string{"a", "b", "c", "d"}, store.SliceOfList("list", 0, -1), "Received data should match expected count")
}

func TestHandleLpushListAppend(t *testing.T) {
	store := StoreWithList("list", []string{"a"})
	conn, cleanup := setupTestConnectionWithStore(t, store)
	defer cleanup()

	_, err := conn.Write([]byte("*5\r\n$4\r\nLPUSH\r\n$5\r\nlist\r\n$1\r\nb\r\n$1\r\nc\r\n$1\r\nd\r\n"))
	if err != nil {
		t.Fatal(err)
	}
	buffer, totalRead := readWithDeadline(t, conn, 10)

	assert.Equal(t, respInteger(4), string(buffer[:totalRead]), "Received data should match expected count")
	assert.Equal(t, []string{"d", "c", "b", "a"}, store.SliceOfList("list", 0, -1), "Received data should match expected count")
}

func TestHandleListRangeEmptyList(t *testing.T) {
	store := StoreWithList("list", []string{})
	conn, cleanup := setupTestConnectionWithStore(t, store)
	defer cleanup()

	_, err := conn.Write([]byte("*4\r\n$4\r\nLRANGE\r\n$5\r\nlist\r\n$1\r\n0\r\n$1\r\n1\r\n"))
	if err != nil {
		t.Fatal(err)
	}
	buffer, totalRead := readWithDeadline(t, conn, 10)

	assert.Equal(t, respArray([]string{}), string(buffer[:totalRead]), "Received data should match expected count")
}

func TestHandleListRangeGoodIndices(t *testing.T) {
	store := StoreWithList("list", []string{"a", "b"})
	conn, cleanup := setupTestConnectionWithStore(t, store)
	defer cleanup()

	_, err := conn.Write([]byte("*4\r\n$4\r\nLRANGE\r\n$5\r\nlist\r\n$1\r\n0\r\n$1\r\n1\r\n"))
	if err != nil {
		t.Fatal(err)
	}
	buffer, totalRead := readWithDeadline(t, conn, 10)

	assert.Equal(t, respArray([]string{"a", "b"}), string(buffer[:totalRead]), "Received data should match expected count")
}

func TestHandleListRangeExceedingStop(t *testing.T) {
	store := StoreWithList("list", []string{"a", "b"})
	conn, cleanup := setupTestConnectionWithStore(t, store)
	defer cleanup()

	_, err := conn.Write([]byte("*4\r\n$4\r\nLRANGE\r\n$5\r\nlist\r\n$1\r\n0\r\n$3\r\n100\r\n"))
	if err != nil {
		t.Fatal(err)
	}
	buffer, totalRead := readWithDeadline(t, conn, 10)

	assert.Equal(t, respArray([]string{"a", "b"}), string(buffer[:totalRead]), "Received data should match expected count")
}

func TestHandleListRangeStartExceedingLength(t *testing.T) {
	store := StoreWithList("list", []string{"a", "b"})
	conn, cleanup := setupTestConnectionWithStore(t, store)
	defer cleanup()

	_, err := conn.Write([]byte("*4\r\n$4\r\nLRANGE\r\n$5\r\nlist\r\n$1\r\n2\r\n$3\r\n100\r\n"))
	if err != nil {
		t.Fatal(err)
	}
	buffer, totalRead := readWithDeadline(t, conn, 10)

	assert.Equal(t, respArray([]string{}), string(buffer[:totalRead]), "Received data should match expected count")

}

func TestHandleListRangeStartExceedingStop(t *testing.T) {
	store := StoreWithList("list", []string{"a", "b"})
	conn, cleanup := setupTestConnectionWithStore(t, store)
	defer cleanup()

	_, err := conn.Write([]byte("*4\r\n$4\r\nLRANGE\r\n$5\r\nlist\r\n$1\r\n2\r\n$1\r\n1\r\n"))
	if err != nil {
		t.Fatal(err)
	}
	buffer, totalRead := readWithDeadline(t, conn, 10)

	assert.Equal(t, respArray([]string{}), string(buffer[:totalRead]), "Received data should match expected count")
}

func TestHandleLlenEmptyList(t *testing.T) {
	store := StoreWithList("list", []string{})
	conn, cleanup := setupTestConnectionWithStore(t, store)
	defer cleanup()

	_, err := conn.Write([]byte("*2\r\n$4\r\nLLEN\r\n$5\r\nlist\r\n"))
	if err != nil {
		t.Fatal(err)
	}
	buffer, totalRead := readWithDeadline(t, conn, 20)

	assert.Equal(t, respInteger(0), string(buffer[:totalRead]), "Received data should match expected count")
}

func TestHandleLlen(t *testing.T) {
	store := StoreWithList("list", []string{"a", "b"})
	conn, cleanup := setupTestConnectionWithStore(t, store)
	defer cleanup()

	_, err := conn.Write([]byte("*2\r\n$4\r\nLLEN\r\n$5\r\nlist\r\n"))
	if err != nil {
		t.Fatal(err)
	}
	buffer, totalRead := readWithDeadline(t, conn, 20)

	assert.Equal(t, respInteger(2), string(buffer[:totalRead]), "Received data should match expected count")
}

func TestHandleLpopEmptyList(t *testing.T) {
	store := StoreWithList("list", []string{})
	conn, cleanup := setupTestConnectionWithStore(t, store)
	defer cleanup()

	_, err := conn.Write([]byte("*2\r\n$4\r\nLPOP\r\n$5\r\nlist\r\n"))
	if err != nil {
		t.Fatal(err)
	}
	buffer, totalRead := readWithDeadline(t, conn, 20)

	assert.Equal(t, bulkStringNull(), string(buffer[:totalRead]), "Received data should match expected count")
}

func TestHandleLpopOneElement(t *testing.T) {
	store := StoreWithList("list", []string{"a", "b"})
	conn, cleanup := setupTestConnectionWithStore(t, store)
	defer cleanup()

	_, err := conn.Write([]byte("*2\r\n$4\r\nLPOP\r\n$5\r\nlist\r\n"))
	if err != nil {
		t.Fatal(err)
	}
	buffer, totalRead := readWithDeadline(t, conn, 20)

	assert.Equal(t, bulkString("a"), string(buffer[:totalRead]), "Received data should match expected count")
	assert.Equal(t, store.ListLen("list"), 1, "The list should have only one element now")
}

func TestHandleLpopMultipleElements(t *testing.T) {
	store := StoreWithList("list", []string{"a", "b", "c"})
	conn, cleanup := setupTestConnectionWithStore(t, store)
	defer cleanup()

	_, err := conn.Write([]byte("*3\r\n$4\r\nLPOP\r\n$5\r\nlist\r\n$1\r\n2\r\n"))
	if err != nil {
		t.Fatal(err)
	}
	buffer, totalRead := readWithDeadline(t, conn, 20)

	assert.Equal(t, respArray([]string{"a", "b"}), string(buffer[:totalRead]), "Received data should match expected count")
	assert.Equal(t, store.ListLen("list"), 1, "The list should have only one element now")
}

func TestHandleBlpop(t *testing.T) {
	store := StoreWithList("list", []string{})
	conn1, cleanup := setupTestConnectionWithStore(t, store)
	defer cleanup()
	conn2, cleanup := setupTestConnectionWithStore(t, store)
	defer cleanup()

	// Ask for BLPOP on conn1
	_, err := conn1.Write([]byte("*3\r\n$5\r\nBLPOP\r\n$4\r\nlist\r\n$1\r\n0\r\n"))
	if err != nil {
		t.Fatal(err)
	}

	buffer, totalRead, err := readWithDeadlineError(t, conn1, 10)
	if err == nil {
		t.Fatal("Expected a timeout error")
	}
	if totalRead > 0 {
		t.Fatal("Expected no bytes during blocking request")
	}
	
	// Add el on conn2
	_, err = conn2.Write([]byte("*3\r\n$5\r\nRPUSH\r\n$4\r\nlist\r\n$1\r\na\r\n"))
	if err != nil {
		t.Fatal(err)
	}

	buffer, totalRead = readWithDeadline(t, conn2, 10)
	assert.Equal(t, respInteger(1), string(buffer[:totalRead]), "Received data should match expected count")

	buffer, totalRead = readWithDeadline(t, conn1, 10)
	assert.Equal(t, "*2\r\n$4\r\nlist\r\n$1\r\na\r\n", string(buffer[:totalRead]), "Received data should match expected count")
}

func TestBlpopRaceMultipleWaiters(t *testing.T) {
	store := StoreWithList("list", []string{})
	conn1, cleanup1 := setupTestConnectionWithStore(t, store)
	defer cleanup1()
	conn2, cleanup2 := setupTestConnectionWithStore(t, store)
	defer cleanup2()
	conn3, cleanup3 := setupTestConnectionWithStore(t, store)
	defer cleanup3()

	// Two clients waiting on BLPOP
	_, err := conn1.Write([]byte("*3\r\n$5\r\nBLPOP\r\n$4\r\nlist\r\n$1\r\n0\r\n"))
	if err != nil {
		t.Fatal(err)
	}
	_, err = conn2.Write([]byte("*3\r\n$5\r\nBLPOP\r\n$4\r\nlist\r\n$1\r\n0\r\n"))
	if err != nil {
		t.Fatal(err)
	}

	// Verify both are blocking
	_, _, err = readWithDeadlineError(t, conn1, 10)
	if err == nil {
		t.Fatal("Expected conn1 to block")
	}
	_, _, err = readWithDeadlineError(t, conn2, 10)
	if err == nil {
		t.Fatal("Expected conn2 to block")
	}

	// Push two elements — each waiter should get one
	_, err = conn3.Write([]byte("*4\r\n$5\r\nRPUSH\r\n$4\r\nlist\r\n$1\r\na\r\n$1\r\nb\r\n"))
	if err != nil {
		t.Fatal(err)
	}

	buffer, totalRead := readWithDeadline(t, conn3, 10)
	assert.Equal(t, respInteger(2), string(buffer[:totalRead]), "RPUSH should return total pushed")

	validResponses := []string{
		"*2\r\n$4\r\nlist\r\n$1\r\na\r\n",
		"*2\r\n$4\r\nlist\r\n$1\r\nb\r\n",
	}

	buffer, totalRead = readWithDeadline(t, conn1, 10)
	conn1Response := string(buffer[:totalRead])
	assert.Contains(t, validResponses, conn1Response, "conn1 should receive one element")

	buffer, totalRead = readWithDeadline(t, conn2, 10)
	conn2Response := string(buffer[:totalRead])
	assert.Contains(t, validResponses, conn2Response, "conn2 should receive one element")

	assert.NotEqual(t, conn1Response, conn2Response, "conn1 and conn2 should receive different elements")
}

func TestBlpopRaceElementsAlreadyPresent(t *testing.T) {
	store := StoreWithList("list", []string{"a", "b"})
	conn1, cleanup1 := setupTestConnectionWithStore(t, store)
	defer cleanup1()

	// BLPOP on non-empty list should return immediately
	_, err := conn1.Write([]byte("*3\r\n$5\r\nBLPOP\r\n$4\r\nlist\r\n$1\r\n0\r\n"))
	if err != nil {
		t.Fatal(err)
	}

	buffer, totalRead := readWithDeadline(t, conn1, 10)
	assert.Equal(t, "*2\r\n$4\r\nlist\r\n$1\r\na\r\n", string(buffer[:totalRead]), "Should return immediately with first element")
}

func TestBlpopRaceConcurrentPush(t *testing.T) {
	store := StoreWithList("list", []string{})
	conn1, cleanup1 := setupTestConnectionWithStore(t, store)
	defer cleanup1()
	conn2, cleanup2 := setupTestConnectionWithStore(t, store)
	defer cleanup2()
	conn3, cleanup3 := setupTestConnectionWithStore(t, store)
	defer cleanup3()

	// conn1 blocks on BLPOP
	_, err := conn1.Write([]byte("*3\r\n$5\r\nBLPOP\r\n$4\r\nlist\r\n$1\r\n0\r\n"))
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = readWithDeadlineError(t, conn1, 10)
	if err == nil {
		t.Fatal("Expected conn1 to block")
	}

	// conn2 and conn3 push concurrently
	conn2.Write([]byte("*3\r\n$5\r\nRPUSH\r\n$4\r\nlist\r\n$1\r\na\r\n"))
	conn3.Write([]byte("*3\r\n$5\r\nRPUSH\r\n$4\r\nlist\r\n$1\r\nb\r\n"))

	// conn1 should receive exactly one element
	buffer, totalRead := readWithDeadline(t, conn1, 10)
	response := string(buffer[:totalRead])
	validResponses := []string{
		"*2\r\n$4\r\nlist\r\n$1\r\na\r\n",
		"*2\r\n$4\r\nlist\r\n$1\r\nb\r\n",
	}
	assert.Contains(t, validResponses, response, "conn1 should receive exactly one element")

	// One element should remain in the list
	assert.Equal(t, 1, store.ListLen("list"), "One element should remain in the list")
}