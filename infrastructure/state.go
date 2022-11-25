package infrastructure

import (
  "encoding/hex"
  "crypto/sha256"
)

const (
  // State Key mapping FIDs to Plaintext URLs
  RedisFunctionalToURLKey = "content:functional:"

  /* State Key mapping Safe URLs to FIDs. Note URLs must be encoded in a safe format
  that doesn't include special characters, specifically ":" */
  RedisURLToFunctionalKey = "content:url:"

  // State Key mapping Safe URLs to a set of all filesystem resources created under URL
  RedisURLToResourcesKey = "content:resources:"
)

/* URLToSafeName converts URL with possible unsafe
characters to a unique hex string 24 bytes long */
func URLToSafeName(url string) string {
  sum := sha256.Sum224([]byte(url))
  safe := hex.EncodeToString(sum[:])
  return safe
}
