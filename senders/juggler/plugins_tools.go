package juggler

import (
	"strings"

	"github.com/yuin/gluare"
	lua "github.com/yuin/gopher-lua"
)

func split(l *lua.LState) int {
	s := l.CheckString(1)
	sep := l.CheckString(2)
	splited := strings.Split(s, sep)

	t := l.NewTable()
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

// PreloadTools preload go functions in lua global environment
func PreloadTools(l *lua.LState) error {
	l.SetGlobal("split", l.NewFunction(split))
	l.SetGlobal("replace", l.NewFunction(replace))
	l.PreloadModule("re", gluare.Loader)
	return nil
}
