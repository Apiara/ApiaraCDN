package infrastructure

import (
	"crypto/sha256"
	"encoding/hex"
)

/*
URLToSafeName converts URL with possible unsafe
characters to a unique hex string 24 bytes long
*/
func URLToSafeName(url string) string {
	sum := sha256.Sum224([]byte(url))
	safe := hex.EncodeToString(sum[:])
	return safe
}
