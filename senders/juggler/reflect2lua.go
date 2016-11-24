package juggler

import (
	"fmt"
	"reflect"
	"strconv"

	"github.com/combaine/combaine/common/logger"
	"github.com/combaine/combaine/common/tasks"
	lua "github.com/yuin/gopher-lua"
)

func (js *JugglerSender) dataToLuaTable(in tasks.DataType) (*lua.LTable, error) {
	out := js.state.NewTable()
	for host, item := range in {
		tableHost := js.state.NewTable()
		out.RawSetString(host, tableHost)
		for metric, value := range item {
			rv := reflect.ValueOf(value)
			if err := dump(js.state, tableHost, metric, rv); err != nil {
				return nil, err
			}
		}
	}
	return out, nil
}

func dump(l *lua.LState, t *lua.LTable, key string, rv reflect.Value) (err error) {

	switch rv.Kind() {
	case reflect.Slice, reflect.Array:
		err = dumpSlice(l, t, key, rv)
	case reflect.Map:
		err = dumpMap(l, t, key, rv)
	default:
		v, err := dumpPoint(rv)
		if err != nil {
			return err
		}
		t.RawSetString(key, v)
	}
	return
}

func dumpMap(l *lua.LState, t *lua.LTable, k string, rv reflect.Value) (err error) {
	inTable := l.NewTable()
	t.RawSetString(k, inTable)

	keys := rv.MapKeys()
	for _, key := range keys {
		itemInterface := reflect.ValueOf(rv.MapIndex(key).Interface())
		//logger.Infof("Item of key %s is: %v", key, itemInterface.Kind())

		err = dump(l, inTable, key.String(), itemInterface)
		if err != nil {
			return
		}
	}
	return nil
}

func dumpSlice(l *lua.LState, t *lua.LTable, k string, rv reflect.Value) error {
	if len(js.Fields) == 0 || len(js.Fields) != rv.Len() {
		msg := fmt.Sprintf("%s Unable to send a slice. Fields len %d, len of value %d", js.id, len(js.Fields), rv.Len())
		logger.Errf(msg)
		return fmt.Errorf(msg)
	}
	inTable := l.NewTable()
	t.RawSetString(k, inTable)

	for i := 0; i < rv.Len(); i++ {
		item := reflect.ValueOf(rv.Index(i).Interface())
		v, err := dumpPoint(item)
		if err != nil {
			return err
		}
		inTable.Insert(i+1, v)
	}
	return nil
}

func dumpPoint(value reflect.Value) (ret lua.LNumber, err error) {

	switch value.Kind() {
	case reflect.Float32, reflect.Float64:
		ret = lua.LNumber(value.Float())
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		ret = lua.LNumber(float64(value.Int()))
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32,
		reflect.Uint64, reflect.Uintptr:
		ret = lua.LNumber(float64(value.Uint()))
	case reflect.String:
		num, err := strconv.ParseFloat(value.String(), 64)
		if err == nil {
			ret = lua.LNumber(num)
		}
	default:
		err = fmt.Errorf("%s value %s is Not a Number: %v", js.id, value.Kind(), value)
	}
	return
}
