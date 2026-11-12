package workload

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
)

// Names and label values are fixed-length hashes of exact identities. Neither
// punctuation, case folding nor truncation can alias distinct caller identities.
func runtimeResourceName(instanceID string) string { return "wl-" + identityHash(instanceID)[:56] }
func runtimeLabelValue(value string) string        { return identityHash(value)[:56] }
func identityHash(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}

func identityKey(parts ...string) string { b, _ := json.Marshal(parts); return identityHash(string(b)) }
