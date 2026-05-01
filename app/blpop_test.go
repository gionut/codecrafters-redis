package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

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