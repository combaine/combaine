package juggler

import (
	"time"

	"github.com/combaine/combaine/repository"
	"github.com/globalsign/mgo"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

// EventsStore juggler events history
type EventsStore interface {
	// Connect store
	Connect() error
	// Update and get events history
	Push(key, event string, historyLen int) ([]string, error)
	// Close juggler events store
	Close()
}
type pluginEventsStore struct {
	session   *mgo.Session
	config    *pluginEventsStoreConfig
	queryChan chan storeRequest
}

type storeResult struct {
	history []string
	err     error
}

type storeRequest struct {
	key     string
	event   string
	history int
	out     chan storeResult
}

type eventHistory struct {
	ID     string `bson:"_id"`
	Events []string
}

type pluginEventsStoreConfig struct {
	Cluster  string
	Fetcher  repository.PluginConfig
	Database string
	AuthDB   string
	User     string
	Password string
}

var eventsStore = &pluginEventsStore{
	queryChan: make(chan storeRequest),
}

// InitEventsStore initiate global plugin events store
// for save juggler events history and use it as antiflap method
func InitEventsStore(config *pluginEventsStoreConfig) {
	// always create instance
	eventsStore.config = config

	if config.Cluster == "" {
		logrus.Infof("Plugin events store not configured, skip")
		return
	}
	if config.Database == "" {
		config.Database = "combaine"
		logrus.Infof("Plugin event store use default db name 'combaine'")
	}
	if config.AuthDB == "" {
		config.AuthDB = config.Database
	}

	if err := eventsStore.Connect(); err != nil {
		logrus.Errorf("Failed to connect event storage %s", err)
		return
	}
	go eventsStore.worker()
	logrus.Infof("Plugin event store connected sucessfully")
}

// Connect event storage
func (s *pluginEventsStore) Connect() error {
	var (
		session *mgo.Session
		err     error
	)
	session, err = mgo.DialWithTimeout(s.config.Cluster, 10*time.Second)
	if err != nil {
		return errors.Wrap(err, "mongo connect")
	}
	session.SetMode(mgo.Eventual, true)

	if s.config.User != "" {
		err = session.DB(s.config.AuthDB).Login(s.config.User, s.config.Password)
		if err != nil {
			return errors.Wrap(err, "mongo login")
		}
	}
	s.session = session
	return nil
}

// Close events history storage
func (s *pluginEventsStore) Close() {
	if s.session != nil {
		s.session.Close()
		s.session = nil
	}
}

// io worker
func (s *pluginEventsStore) worker() {
	for q := range s.queryChan {
		mQuery := eventHistory{}
		result := storeResult{}
		c := s.session.DB(s.config.Database).C("events")
		err := c.FindId(q.key).One(&mQuery)
		if err != nil {
			mQuery.ID = q.key
			mQuery.Events = []string{q.event}
			err = c.Insert(mQuery)
			result.history = mQuery.Events
			if err != nil {
				result.err = errors.Wrap(err, "mongo insert query")
			}
		} else {
			if len(mQuery.Events) < q.history {
				mQuery.Events = append(mQuery.Events, "") // add room for one fresh
				q.history = len(mQuery.Events)
			}
			for i := q.history - 1; i > 0; i-- {
				mQuery.Events[i] = mQuery.Events[i-1]
			}
			mQuery.Events[0] = q.event
			mQuery.Events = mQuery.Events[:q.history]
			result.history = mQuery.Events
			_, err = c.UpsertId(q.key, mQuery)
			if err != nil {
				result.err = errors.Wrap(err, "mongo upsert query")
			}
		}

		select {
		case q.out <- storeResult{history: mQuery.Events}:
		default:
		}
		// https://godoc.org/gopkg.in/mgo.v2#Session.Refresh
		s.session.Refresh()
	}
}

// Push new event and get back current history
func (s *pluginEventsStore) Push(key, event string, history int, deadline time.Time) ([]string, error) {
	if s.session == nil {
		return nil, errors.New("plugin store not connected")
	}
	query := storeRequest{
		key:     key,
		event:   event,
		history: history,
		out:     make(chan storeResult),
	}
	select {
	case s.queryChan <- query:
	case <-time.After(time.Until(deadline)):
		return nil, errors.New("store send DeadlineExceeded")
	}
	select {
	case <-time.After(time.Until(deadline)):
		return nil, errors.New("store read DeadlineExceeded")
	case resp := <-query.out:
		return resp.history, resp.err
	}
}
