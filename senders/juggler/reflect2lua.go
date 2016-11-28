package juggler

import (
	"fmt"
	"reflect"
	"strconv"

	"github.com/combaine/combaine/common/tasks"
	lua "github.com/yuin/gopher-lua"
)

func dataToLuaTable(l *lua.LState, in tasks.DataType) (*lua.LTable, error) {
	out := l.NewTable()
	for host, item := range in {
		tableHost := l.NewTable()
		out.RawSetString(host, tableHost)
		for metric, value := range item {
			rv := reflect.ValueOf(value)
			if err := dumpToLuaTable(l, tableHost, metric, rv); err != nil {
				return nil, err
			}
		}
	}
	return out, nil
}

func dumpToLuaTable(l *lua.LState, t *lua.LTable, key string, rv reflect.Value) (err error) {

	switch rv.Kind() {
	case reflect.Slice, reflect.Array:
		err = dumpSliceToLuaTable(l, t, key, rv)
	case reflect.Map:
		err = dumpMapToLuaTable(l, t, key, rv)
	default:
		v, err := dumpItemToLuaNumber(rv)
		if err != nil {
			return err
		}
		t.RawSetString(key, v)
	}
	return
}

func dumpMapToLuaTable(l *lua.LState, t *lua.LTable, k string, rv reflect.Value) (err error) {
	inTable := l.NewTable()
	t.RawSetString(k, inTable)

	keys := rv.MapKeys()
	for _, key := range keys {
		itemInterface := reflect.ValueOf(rv.MapIndex(key).Interface())
		//logger.Infof("Item of key %s is: %v", key, itemInterface.Kind())

		err = dumpToLuaTable(l, inTable, key.String(), itemInterface)
		if err != nil {
			return
		}
	}
	return nil
}

func dumpSliceToLuaTable(l *lua.LState, t *lua.LTable, k string, rv reflect.Value) error {
	inTable := l.NewTable()
	t.RawSetString(k, inTable)

	for i := 0; i < rv.Len(); i++ {
		item := reflect.ValueOf(rv.Index(i).Interface())
		v, err := dumpItemToLuaNumber(item)
		if err != nil {
			return err
		}
		inTable.Insert(i+1, v)
	}
	return nil
}

func dumpItemToLuaNumber(value reflect.Value) (ret lua.LNumber, err error) {

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
		err = fmt.Errorf("value %s is Not a Number: %v", value.Kind(), value)
	}
	return
}
