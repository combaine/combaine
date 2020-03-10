package utils

import (
	"bytes"
	"crypto/md5"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"time"

	"github.com/vmihailenco/msgpack"
)

var (
	// sha1ver revision used to build the program
	sha1ver = "Undefined"
	// buildTime when the executable was built
	buildTime  = "Undefined"
	versionTag = "Undefined"
)

// Pack is helper for encode data in to msgpack
func Pack(v interface{}) ([]byte, error) {
	var buf bytes.Buffer
	err := msgpack.NewEncoder(&buf).Encode(v)
	return buf.Bytes(), err
}

// Unpack is helper for decoding data in to msgpack
func Unpack(data []byte, v interface{}) error {
	return msgpack.NewDecoder(bytes.NewReader(data)).Decode(v)
}

// GetType return type of plugin or error if field type not present
func GetType(cfg map[string]interface{}) (string, error) {
	value, ok := cfg["type"]
	if !ok {
		return "", errors.New("Missing field type")
	}
	Type, ok := value.(string)
	if !ok {
		return "", errors.New("type field isn't string")
	}
	return Type, nil
}

// InterfaceToString print bytes with %q and other with %v format
func InterfaceToString(v interface{}) (s string) {
	switch v := v.(type) {
	case []byte:
		s = fmt.Sprintf("%q", v)
	default:
		s = fmt.Sprintf("%v", v)
	}
	return
}

// NameStack helper type for stacking metrics name
type NameStack []string

// Push add one name to metric path
func (n *NameStack) Push(item string) {
	*n = append(*n, item)
}

// Pop remove one name from metric path
func (n *NameStack) Pop() (item string) {
	item, *n = (*n)[len(*n)-1], (*n)[:len(*n)-1]
	return item
}

// GetSubgroupName helper for exstracting proper subgroup name from tags
func GetSubgroupName(tags map[string]string) (string, error) {
	subgroup, ok := tags["name"]
	if !ok {
		return "", fmt.Errorf("Failed to get data tag 'name': %q", tags)
	}

	t, ok := tags["type"]
	if !ok {
		return "", fmt.Errorf("Failed to get data tag 'type': %q", tags)
	}

	if t == "datacenter" {
		meta, ok := tags["metahost"]
		if !ok {
			return "", fmt.Errorf("Failed to get data tag 'metahost': %q", tags)
		}
		subgroup = meta + "-" + subgroup // meta.host.name + DC1
	}
	return subgroup, nil
}

// GenerateSessionID = uuid.New
func GenerateSessionID() string {
	var buf = make([]byte, 0, 16)
	buf = strconv.AppendInt(buf, time.Now().UnixNano(), 10)
	buf = strconv.AppendInt(buf, rand.Int63(), 10)
	val := md5.Sum(buf)
	return fmt.Sprintf("%x", string(val[:]))
}

// Hostname return node hostname or panic on errors
func Hostname() string {
	hostname, err := os.Hostname()
	if err != nil {
		panic(err)
	}
	return hostname
}

// GetVersionString of the bianry
func GetVersionString() string {
	return fmt.Sprintf("Build on %s from sha1 %s (version %s)", buildTime, sha1ver, versionTag)
}
