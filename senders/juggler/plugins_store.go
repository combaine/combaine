package juggler

import (
	"github.com/combaine/combaine/common/logger"
	"github.com/pkg/errors"
	mgo "gopkg.in/mgo.v2"
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
	AuthDB   string `yaml:"authdb"`
	User     string `yaml:"user"`
	Password string `yaml:"password"`
}

var eventsStore EventsStore

// InitEventsStore initiate global plugin events store
// for save juggler events history and use it as antiflap method
func InitEventsStore(config *pluginEventsStoreConfig) {
	if config.Cluster == "" {
		logger.Infof("Plugin events store not configured, skip")
		return
	}
	if config.Database == "" {
		config.Database = "combaine"
		logger.Infof("Plugin event store use default db name 'combaine'")
	}
	if config.AuthDB == "" {
		config.AuthDB = config.Database
	}

	store := &pluginEventsStore{config: config}
	if err := store.Connect(); err != nil {
		logger.Errf("Failed to connect event storage %s", err)
		return
	}
	eventsStore = store
	logger.Infof("Plugin event store connected sucessfully")
}

// Connect event storage
func (s *pluginEventsStore) Connect() error {
	var err error
	s.session, err = mgo.Dial(s.config.Cluster)
	if err != nil {
		return errors.Wrap(err, "mongo connect")
	}
	err = s.session.DB(s.config.AuthDB).Login(s.config.User, s.config.Password)
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
	err := c.FindId(key).One(&result)
	if err != nil {
		result.Id = key
		result.Events = []string{event}
		err = c.Insert(result)
		if err != nil {
			return nil, errors.Wrap(err, "mongo insert query")
		}
		return result.Events, nil
	}
	if len(result.Events) < history {
		result.Events = append(result.Events, "") // add room for one fresh
		history = len(result.Events)
	}
	for i := history - 1; i > 0; i-- {
		result.Events[i] = result.Events[i-1]
	}
	result.Events[0] = event
	result.Events = result.Events[:history]
	_, err = c.UpsertId(key, result)
	if err != nil {
		return nil, errors.Wrap(err, "mongo update query")
	}
	return result.Events, nil
}
