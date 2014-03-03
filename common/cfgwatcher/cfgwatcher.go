package cfgwatcher

import (
	"fmt"
	"log"

	"github.com/howeyc/fsnotify"
)

type SimpleCfgWatcher interface {
	Watch(cfgpath string) (chan error, error)
	Close()
}

type simpleCfgWatcher struct {
	w          *fsnotify.Watcher
	onClose    chan bool
	isWatching bool
}

func (s *simpleCfgWatcher) Watch(cfgpath string) (ch chan error, err error) {
	if s.isWatching {
		err = fmt.Errorf("Watcher has already exists")
		return
	}

	err = s.w.Watch(cfgpath)
	if err != nil {
		return
	}

	ch = make(chan error)

	go func() {
		for {
			select {
			case ev := <-s.w.Event:
				log.Println("Event: ", ev.String())
				ch <- nil
			case err := <-s.w.Error:
				log.Println("WathcerError: ", err)
				ch <- err
			case <-s.onClose:
				return
			}
		}
	}()
	return
}

func (s *simpleCfgWatcher) Close() {
	s.w.Close()
	close(s.onClose)
}

func NewSimpleCfgwatcher() (sw SimpleCfgWatcher, err error) {
	w, err := fsnotify.NewWatcher()
	if err != nil {
		return
	}

	sw = &simpleCfgWatcher{
		w:          w,
		onClose:    make(chan bool),
		isWatching: false,
	}
	return
}
