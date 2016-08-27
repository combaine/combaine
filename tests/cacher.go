package tests

import (
	"github.com/cocaine/cocaine-framework-go/cocaine"
	"github.com/combaine/combaine/common"
)

type Service interface {
	Call(name string, args ...interface{}) chan cocaine.ServiceResult
	Close()
}

var Spy = make(chan []interface{}, 2) // avoid blocking
var Rake = make(chan error, 2)        // avoid blocking

type serviceResult struct {
	err  error
	data []byte
}

func (sr serviceResult) Extract(i interface{}) error {
	return common.Unpack(sr.data, i) // unpack double packed payload
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

	data, err := common.Pack(args[1].([]byte)) // double pack

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

func NewService(string, ...interface{}) (Service, error) {
	return &tService{r: make(chan cocaine.ServiceResult)}, nil
}
