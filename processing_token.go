package main

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
)

// newProcessingToken creates a cryptographically random identifier for a single
// task claim. It must be stored atomically with the claim before being used for
// heartbeat, completion, or archival fencing.
func newProcessingToken() (string, error) {
	var bytes [16]byte
	if _, err := rand.Read(bytes[:]); err != nil {
		return "", fmt.Errorf("generate processing token: %w", err)
	}
	// RFC 4122 version 4 UUID, represented as a 36-character string.
	bytes[6] = (bytes[6] & 0x0f) | 0x40
	bytes[8] = (bytes[8] & 0x3f) | 0x80
	var chars [36]byte
	hex.Encode(chars[0:8], bytes[0:4])
	chars[8] = '-'
	hex.Encode(chars[9:13], bytes[4:6])
	chars[13] = '-'
	hex.Encode(chars[14:18], bytes[6:8])
	chars[18] = '-'
	hex.Encode(chars[19:23], bytes[8:10])
	chars[23] = '-'
	hex.Encode(chars[24:36], bytes[10:16])
	return string(chars[:]), nil
}
