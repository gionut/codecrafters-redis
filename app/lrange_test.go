package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

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
