package juggler

import (
	"strings"

	lua "github.com/yuin/gopher-lua"
)

func Split(l *lua.LState) int {
	s := l.CheckString(1)
	sep := l.CheckString(2)
	splited := strings.Split(s, sep)

	t := l.NewTable()
	for _, substr := range splited {
		t.Append(lua.LString(substr))
	}
	l.Push(t)
	return 1
}

// PreloadTools preload go functions in lua global environment
func PreloadTools(l *lua.LState) error {
	l.SetGlobal("split", l.NewFunction(Split))
	return nil
}
