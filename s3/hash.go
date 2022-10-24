package s3

import (
	"crypto/hmac"
	"crypto/md5"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
)

var EmptyStringSHA256Hex = SHA256Hex(nil)

// MD5B64 calculates the md5 base46 hash for a given string
func MD5B64(in []byte) string {
	h := md5.New()
	fmt.Fprintf(h, "%s", in)
	return base64.StdEncoding.EncodeToString(h.Sum(nil))
}

// SHA256Hex calculates the sha256 hex hash for a given string
func SHA256Hex(in []byte) string {
	h := sha256.New()
	fmt.Fprintf(h, "%s", in)
	return fmt.Sprintf("%x", h.Sum(nil))
}

// HMAC calculates the sha256 hmac for a given slice of bytes
func HMAC(key, data []byte) []byte {
	h := hmac.New(sha256.New, key)
	h.Write(data)
	return h.Sum(nil)
}
