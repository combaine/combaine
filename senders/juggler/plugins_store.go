package juggler

import (
	"github.com/combaine/combaine/common/logger"
	"github.com/pkg/errors"
	mgo "gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

// juggler events history
type EventsStore interface {
	// Connect store
	Connect() error
	// Update and get events history
	Push(key, event string, historyLen int) ([]string, error)
	// Close juggler events store
	Close()
}
type pluginEventsStore struct {
	session *mgo.Session
	config  *pluginEventsStoreConfig
}

type eventHistory struct {
	Id     string `bson:"_id"`
	Events []string
}

type pluginEventsStoreConfig struct {
	Cluster  string `yaml:"cluster"`
	Database string `yaml:"database"`
	User     string `yaml:"user"`
	Password string `yaml:"password"`
}

var eventsStore EventsStore

// InitEventsStore initiate global plugin events store
// for save juggler events history and use it as antiflap method
func InitEventsStore(config *pluginEventsStoreConfig) {
	eventsStore = &pluginEventsStore{config: config}
	if err := eventsStore.Connect(); err != nil {
		logger.Errf("Failed to connect event storage %s", err)
	}
}

// Connect event storage
func (s *pluginEventsStore) Connect() error {
	var err error
	s.session, err = mgo.Dial(s.config.Cluster)
	if err != nil {
		return errors.Wrap(err, "mongo connect")
	}
	err = s.session.DB("admin").Login(s.config.User, s.config.Password)
	if err != nil {
		return errors.Wrap(err, "mongo login")
	}
	return nil
}

// Close events history storage
func (s *pluginEventsStore) Close() {
	if s.session != nil {
		s.session.Close()
	}
}

// Push new event and get back current history
func (s *pluginEventsStore) Push(key, event string, history int) ([]string, error) {
	result := eventHistory{}
	c := s.session.DB(s.config.Database).C("events")
	err := c.Find(bson.M{"_id": key}).One(&result)
	if err != nil {
		return nil, errors.Wrap(err, "mongo findOne query")
	}
	if len(result.Events) < history {
		history = len(result.Events)
	}
	for i := history - 1; i > 0; i-- {
		result.Events[i] = result.Events[i-1]
	}
	result.Events[0] = event
	result.Events = result.Events[:history]
	_, err = c.UpsertId(result.Id, result)
	if err != nil {
		return nil, errors.Wrap(err, "mongo update query")
	}
	return result.Events, nil
}
