package juggler

import (
	"errors"
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

func luaResultToJugglerEvents(defaultLevel int, result *lua.LTable) ([]jugglerEvent, error) {
	if result == nil {
		return nil, errors.New("lua plugin result is nil")
	}

	var errs []error
	var events []jugglerEvent
	result.ForEach(func(k lua.LValue, v lua.LValue) {
		lt, ok := v.(*lua.LTable)
		if !ok {
			errs = append(errs, fmt.Errorf("Failed to convert: result[%s]=%s is not lua table",
				lua.LVAsString(k), lua.LVAsString(v)))
		}
		je := jugglerEvent{}
		if je.Host = lua.LVAsString(lt.RawGetString("host")); je.Host == "" {
			je.Host = "UnknownHost"
		}
		if je.Description = lua.LVAsString(lt.RawGetString("description")); je.Description == "" {
			je.Description = "no trigger description"
		}
		if je.Service = lua.LVAsString(lt.RawGetString("service")); je.Service == "" {
			je.Service = "UnknownServce"
		}
		if level := lt.RawGetString("level"); level != lua.LNil {
			je.Level = int(lua.LVAsNumber(level))
		} else {
			je.Level = defaultLevel
		}
		events = append(events, je)
	})
	if errs != nil {
		return nil, fmt.Errorf("%s", errs)
	}
	return events, nil
}
