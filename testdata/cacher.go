package tests

import (
	"fmt"

	"github.com/cocaine/cocaine-framework-go/cocaine"
	"github.com/ugorji/go/codec"
)

var (
	mh codec.MsgpackHandle
	h  = &mh
)

func pack(input interface{}) (buf []byte, err error) {
	err = codec.NewEncoderBytes(&buf, h).Encode(input)
	return
}

func unpack(data []byte, res interface{}) error {
	return codec.NewDecoderBytes(data, h).Decode(res)
}

// Service ...
type Service interface {
	Call(name string, args ...interface{}) chan cocaine.ServiceResult
	Close()
}

// Spy ...
var Spy = make(chan []interface{}, 99) // avoid blocking
// Rake ...
var Rake = make(chan error, 99) // avoid blocking

type serviceResult struct {
	err  error
	data []byte
}

func (sr serviceResult) Extract(i interface{}) error {
	b, _ := pack([]byte("returnError"))
	if string(sr.data) == string(b) {
		return fmt.Errorf("extract error")
	}
	return unpack(sr.data, i)
}

func (sr serviceResult) Err() error {
	return sr.err
}

type tService struct {
	r chan cocaine.ServiceResult
}

func (ts *tService) Call(name string, args ...interface{}) chan cocaine.ServiceResult {
	Spy <- args
	ch := make(chan cocaine.ServiceResult)

	data, err := pack(args[1].([]byte)) // double pack

	// if user want put some error
	select {
	case e, ok := <-Rake:
		if ok {
			err = e
			data = nil
		}
	default:
	}

	go func() {
		ch <- serviceResult{data: data, err: err} // return gotten payload
	}()
	return ch
}

func (ts *tService) Close() {
	close(ts.r)
}

// NewService ...
func NewService(name string, args ...interface{}) (Service, error) {
	if name == "errorService" {
		return nil, fmt.Errorf("ask error service")
	}
	return &tService{r: make(chan cocaine.ServiceResult)}, nil
}
