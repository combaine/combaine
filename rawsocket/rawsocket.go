package rawsocket

import (
	"io/ioutil"
	"net"
)

func Get(host string, port string) (blob []byte, err error) {
	endpoint = JoinHostPort(host, port)
	conn, err := net.Dial("tcp", endpoint)
	if err != nil {
		return
	}
	blob, err = ioutil.ReadAll(conn)
	return
}
