// Code copied from go/internal/flageval (the reference); edit there and re-sync.

package lucida

import (
	"crypto/sha256"
	"encoding/hex"
	"strconv"
)

// HashNumerator returns the first 13 hex chars of sha256(input) as an integer (< 2^52).
func HashNumerator(input string) uint64 {
	sum := sha256.Sum256([]byte(input))
	n, _ := strconv.ParseUint(hex.EncodeToString(sum[:7])[:13], 16, 64)
	return n
}

// Hash maps input to [0,1): HashNumerator / 16^13 (= 2^52, exact in float64).
func Hash(input string) float64 { return float64(HashNumerator(input)) / 4503599627370496 }

func rolloutHash(flagKey, salt, key string) float64 { return Hash(flagKey + "." + salt + "." + key) }

func variantHash(flagKey, salt, key string) float64 {
	return Hash(flagKey + "." + salt + ".variant." + key)
}
