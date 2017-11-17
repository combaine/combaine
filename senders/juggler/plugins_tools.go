package juggler

import (
	"strings"

	"github.com/combaine/combaine/common/logger"
	"github.com/yuin/gluare"
	lua "github.com/yuin/gopher-lua"
)

func split(l *lua.LState) int {
	s := l.CheckString(1)
	sep := l.CheckString(2)
	splited := strings.Split(s, sep)

	t := l.CreateTable(len(splited), 0)
	// TODO: use user data is more efficent here
	for _, substr := range splited {
		t.Append(lua.LString(substr))
	}
	l.Push(t)
	return 1
}

func replace(l *lua.LState) int {
	origin := l.CheckString(1)
	pattern := l.CheckString(2)
	repl := l.CheckString(3)
	res := strings.Replace(origin, pattern, repl, 1)
	l.Push(lua.LString(res))
	return 1
}

func logLoader(id string) func(*lua.LState) int {
	return func(l *lua.LState) int {
		l.Push(l.SetFuncs(l.CreateTable(0, 4), map[string]lua.LGFunction{
			"debug": getLogger(id, "debug"),
			"info":  getLogger(id, "info"),
			"warn":  getLogger(id, "warn"),
			"error": getLogger(id, "error"),
		}))
		return 1
	}
}

func getLogger(id, level string) func(l *lua.LState) int {
	log := logger.Debugf
	switch level {
	case "info":
		log = logger.Infof
	case "warn":
		log = logger.Warnf
	case "error":
		log = logger.Errf
	}
	return func(l *lua.LState) int {
		fmtstr := l.CheckString(1)
		// get the number of arguments passed from lua
		nargs := l.GetTop()
		args := make([]interface{}, nargs-1)
		// lua indexes starts with 1,
		// so we need loop up to nargs to see last argument
		// first argument with index 1 is `fmtstr`
		for i := 2; i <= nargs; i++ {
			args[i-2] = interface{}(l.Get(i))
		}
		log(id+" "+fmtstr, args...)
		return 0
	}
}

func eventsHistoryLoader(id string) func(l *lua.LState) int {
	return func(l *lua.LState) int {
		key := l.CheckString(1)
		event := l.CheckString(2)
		historyLen := l.CheckInt(3)

		if eventsStore == nil {
			l.Push(lua.LNil)
			return 1
		}
		if events, err := eventsStore.Push(key, event, historyLen); err != nil {
			logger.Errf(id+" Failed to update events history: %s", err)
			l.Push(lua.LNil)
		} else {
			if len(events) > 0 {
				eventsTable := l.CreateTable(len(events), 0)
				for _, event := range events {
					eventsTable.Append(lua.LString(event))
				}
				l.Push(eventsTable)
			} else {
				l.Push(lua.LNil)
			}
		}
		return 1
	}
}

// PreloadTools preload go functions in lua global environment
func PreloadTools(id string, l *lua.LState) error {
	l.SetGlobal("split", l.NewFunction(split))
	l.SetGlobal("replace", l.NewFunction(replace))
	l.SetGlobal("events_history", l.NewFunction(eventsHistoryLoader(id)))
	l.PreloadModule("re", gluare.Loader)
	l.PreloadModule("log", logLoader(id))
	return nil
}
