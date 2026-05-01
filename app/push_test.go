package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

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