package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

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