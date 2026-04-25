package main

import (
	"bufio"
	"fmt"
	"strings"
)

// Return a RESP array value "*<number-of-elements>\r\n<element-1>...<element-n>"
func respArray(array []string) string {
    var sb strings.Builder
    fmt.Fprintf(&sb, "*%d\r\n", len(array))
    for _, el := range array {
        fmt.Fprintf(&sb, "$%d\r\n%s\r\n", len(el), el)
    }
    return sb.String()
}

// Return a RESP integer value ":[+|-]<value>\r\n"
func respInteger(value int) string {
    if value < 0 {
		return fmt.Sprintf(":-%d\r\n", value)
	} 
	return fmt.Sprintf(":%d\r\n", value)
}

// Return a bulk string from value "$<length>\r\n<value>\r\n"
func bulkString(value string) string {
    return fmt.Sprintf("$%d\r\n%s\r\n", len(value), value)
}

func bulkStringNull() string {
    return "$-1\r\n"
}

// parseRESP reads a single RESP value (length line + payload line) from the reader.
func parseRESP(reader *bufio.Reader) (string, error) {
	_, err := reader.ReadString('\n') // consume length line
	if err != nil {
		return "", fmt.Errorf("missing length: %w", err)
	}
	payload, err := reader.ReadString('\n')
	if err != nil {
		return "", fmt.Errorf("missing payload: %w", err)
	}
	return strings.TrimRight(payload, "\r\n"), nil
}


// parseCommand reads a full RESP array command from the reader,
// returning the command name and its arguments.
func parseCommand(reader *bufio.Reader, argn int) (string, []string, error) {
	cmd, err := parseRESP(reader)
	if err != nil {
		return "", nil, fmt.Errorf("parsing command: %w", err)
	}

	args := make([]string, 0, argn-1)
	for range argn - 1 {
		arg, err := parseRESP(reader)
		if err != nil {
			return "", nil, fmt.Errorf("parsing arg %d: %w", len(args)+1, err)
		}
		args = append(args, arg)
	}

	return cmd, args, nil
}