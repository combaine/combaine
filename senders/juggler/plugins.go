package juggler

import (
	"errors"
	"fmt"
	"reflect"
	"strconv"

	"github.com/combaine/combaine/common/configs"
	"github.com/combaine/combaine/common/logger"
	"github.com/combaine/combaine/common/tasks"
	lua "github.com/yuin/gopher-lua"
)

type DumperFunc func(reflect.Value) (lua.LValue, error)

type JugglerLevels map[string]int

var jLevels = JugglerLevels{
	"OK":   0,
	"WARN": 1,
	"CRIT": 2,
	"INFO": 3,
}

func jPluginConfigToLuaTable(l *lua.LState, in configs.PluginConfig) (*lua.LTable, error) {
	table := l.NewTable()
	for name, value := range in {
		if val, err := ToLuaValue(l, value, dumperToLuaValue); err == nil {
			table.RawSetString(name, val)
		} else {
			return nil, err
		}
	}
	return table, nil
}

func dataToLuaTable(l *lua.LState, in []tasks.AggregationResult) (*lua.LTable, error) {
	out := l.NewTable()
	for _, item := range in {
		table := l.NewTable()

		tags := l.NewTable()
		for k, v := range item.Tags {
			tags.RawSetString(k, lua.LString(v))
		}
		table.RawSetString("Tags", tags)

		if val, err := ToLuaValue(l, item.Result, dumperToLuaNumber); err == nil {
			table.RawSetString("Result", val)
		} else {
			return nil, err
		}
		out.Append(table)
	}
	return out, nil
}

func ToLuaValue(l *lua.LState, v interface{}, dumper DumperFunc) (lua.LValue, error) {
	rv := reflect.ValueOf(v)

	switch rv.Kind() {
	case reflect.Slice, reflect.Array:
		inTable := l.NewTable()
		for i := 0; i < rv.Len(); i++ {
			item := rv.Index(i).Interface()
			if v, err := ToLuaValue(l, item, dumper); err == nil {
				inTable.Append(v)
			} else {
				return nil, err
			}
		}
		return inTable, nil
	case reflect.Map:
		inTable := l.NewTable()
		for _, key := range rv.MapKeys() {
			item := rv.MapIndex(key).Interface()
			if v, err := ToLuaValue(l, item, dumper); err == nil {
				inTable.RawSetString(fmt.Sprintf("%s", key), v)
			} else {
				return nil, err
			}
		}
		return inTable, nil
	case reflect.Struct:
		inTable := l.NewTable()

		for i := 0; i < rv.NumField(); i++ {
			item := rv.Field(i).Interface()
			if v, err := ToLuaValue(l, item, dumper); err == nil {
				inTable.RawSetString(rv.Type().Field(i).Name, v)
			} else {
				return nil, err
			}
		}
		return inTable, nil
	default:
		if v, err := dumper(rv); err == nil {
			return v, nil
		} else {
			return nil, err
		}
	}
}

func dumperToLuaNumber(value reflect.Value) (ret lua.LValue, err error) {
	switch value.Kind() {
	case reflect.Float32, reflect.Float64:
		ret = lua.LNumber(value.Float())
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		ret = lua.LNumber(float64(value.Int()))
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		ret = lua.LNumber(float64(value.Uint()))
	case reflect.String:
		var num float64
		num, err = strconv.ParseFloat(value.String(), 64)
		if err == nil {
			ret = lua.LNumber(num)
		}
	default:
		err = fmt.Errorf("value %s is Not a Number: %v", value.Kind(), value)
	}
	return
}

func dumperToLuaValue(value reflect.Value) (ret lua.LValue, err error) {
	switch value.Kind() {
	case reflect.Float32, reflect.Float64:
		ret = lua.LNumber(value.Float())
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		ret = lua.LNumber(value.Int())
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		ret = lua.LNumber(value.Uint())
	case reflect.String:
		ret = lua.LString(value.String())
	default:
		err = fmt.Errorf("value %s is Not a Number or String: %v", value.Kind(), value)
	}
	return
}

// luaResultToJugglerEvents convert well known type of lua plugin result
// in to go types
func (js *jugglerSender) luaResultToJugglerEvents(result *lua.LTable) ([]jugglerEvent, error) {
	if result == nil {
		return nil, errors.New("lua plugin result is nil")
	}

	errs := make(map[string]string, 0)
	var events []jugglerEvent
	result.ForEach(func(k lua.LValue, v lua.LValue) {
		lt, ok := v.(*lua.LTable)
		if !ok {
			errs[fmt.Sprintf("Failed to convert: result[%s]=%s is not lua table",
				lua.LVAsString(k), lua.LVAsString(v))] = ""
			return
		}
		je := jugglerEvent{}
		tags, ok := lt.RawGetString("tags").(*lua.LTable)
		if !ok {
			errs["Failed to convert tags to lua table"] = ""
			return
		}
		je.Tags = make(map[string]string, 0)
		tags.ForEach(func(tk lua.LValue, tv lua.LValue) {
			je.Tags[lua.LVAsString(tk)] = lua.LVAsString(tv)
		})
		if _, ok := je.Tags["type"]; !ok {
			name := "UnknownEntity"
			if entity, ok := je.Tags["name"]; ok {
				name = entity
			}
			errs[fmt.Sprintf("Missing tag type for %s", name)] = ""
			return
		}
		if je.Description = lua.LVAsString(lt.RawGetString("description")); je.Description == "" {
			je.Description = "no trigger description"
		}
		if je.Service = lua.LVAsString(lt.RawGetString("service")); je.Service == "" {
			je.Service = "UnknownServce"
		}
		level := lua.LVAsString(lt.RawGetString("level"))
		if l, ok := jLevels[level]; ok {
			je.Level = l
		} else {
			logger.Errf("%s Plugin %s do not return event level, force status to default", js.id, js.Plugin)
			je.Level = jLevels[js.DefaultCheckStatus]
			je.Description = fmt.Sprintf("%s (Force status %s)", je.Description, js.DefaultCheckStatus)
		}

		events = append(events, je)
	})
	if len(errs) > 0 {
		return nil, fmt.Errorf("%s", errs)
	}
	return events, nil
}

// LoadPlugin cleanup lua state global/local environment
// and load lua plugin by name from juggler config section
func LoadPlugin(dir, name string) (*lua.LState, error) {
	file := fmt.Sprintf("%s/%s.lua", dir, name)

	l := lua.NewState()
	if err := PreloadTools(l); err != nil {
		return nil, err
	}
	if err := l.DoFile(file); err != nil {
		return nil, err
	}
	// TODO: overwrite/cleanup globals in lua plugin?
	//		 cache lua state in plugin cache?
	return l, nil
}

// preparePluginEnv add data from aggregate task as global variable in lua
// plugin. Also inject juggler conditions from juggler configs and plugin config
func (js *jugglerSender) preparePluginEnv(data []tasks.AggregationResult) error {
	ltable, err := dataToLuaTable(js.state, data)
	if err != nil {
		return fmt.Errorf("Failed to convert AggregationResult to lua table: %s", err)
	}

	js.state.SetGlobal("payload", ltable)

	levels := make(map[string][]string)
	if js.Conditions.OK != nil {
		levels["OK"] = js.Conditions.OK
	}
	if js.Conditions.INFO != nil {
		levels["INFO"] = js.Conditions.INFO
	}
	if js.Conditions.WARN != nil {
		levels["WARN"] = js.Conditions.WARN
	}
	if js.Conditions.CRIT != nil {
		levels["CRIT"] = js.Conditions.CRIT
	}

	lconditions := js.state.NewTable()
	for name, cond := range levels {
		lcondTable := js.state.NewTable()
		for _, v := range cond {
			lcondTable.Append(lua.LString(v))
		}
		lconditions.RawSetString(name, lcondTable)
	}
	js.state.SetGlobal("conditions", lconditions)

	if lconfig, err := jPluginConfigToLuaTable(js.state, js.JPluginConfig); err != nil {
		return err
	} else {
		js.state.SetGlobal("config", lconfig)
	}
	return nil
}

// runPlugin run lua plugin with prepared environment
// collect, convert and return plugin result
func (js *jugglerSender) runPlugin() ([]jugglerEvent, error) {
	js.state.Push(js.state.GetGlobal("run"))
	if err := js.state.PCall(0, 1, nil); err != nil {
		return nil, fmt.Errorf("Expected 'run' function inside plugin: %s", err)
	}
	result := js.state.ToTable(1)
	events, err := js.luaResultToJugglerEvents(result)
	if err != nil {
		return nil, err
	}
	return events, nil
}
