package juggler

const (
	getCheckUrl = "http://%s/api/checks/checks?do=1&include_children=true&host_name=%s"
	AoUCheckUrl = "http://%s/api/checks/add_or_update?do=1"
)

type JugglerResponse map[ /*hostname*/ string]map[ /*serviceName*/ string]JugglerCheck

type JugglerChildrenCheck struct {
	Instance string `json:"instance"`
	Host     string `json:"host"`
	Type     string `json:"type"`
	Service  string `json:"service"`
}

type JugglerAggregatorKWArgs []byte

type JugglerFlapConfig struct {
	Enable       int64 `codec:"enable" json:"-"`
	FlapTime     int64 `codec:"flap_time" json:"flap_time"`
	StableTime   int64 `codec:"stable_time" json:"stable_time"`
	CriticalTime int64 `codec:"critical_time" json:"critical_time"`
	BoostTime    int64 `codec:"boost_time" json:"boost_time"`
}

type JugglerCheck struct {
	Update           bool                    `json:"-"`
	Host             string                  `json:"host"`
	Service          string                  `json:"service"`
	Description      string                  `json:"description"`
	RefreshTime      int64                   `json:"refresh_time"`
	Ttl              int64                   `json:"ttl"`
	AlertInterval    []int64                 `json:"alert_interval"`
	Aggregator       string                  `json:"aggregator"`
	AggregatorKWArgs JugglerAggregatorKWArgs `json:"aggregator_kwargs"`
	Tags             []string                `json:"tags"`
	Methods          []string                `json:"methods"`
	Children         []JugglerChildrenCheck  `json:"children"`
	Flap             JugglerFlapConfig       `json:"flaps,omitempty"`

	//Active           string                  `json:"active"`
	//ActiveKWArgs     map[string]string       `json:"active_kwargs"`
	//MaxStatus        string                  `json:"max_status"`
	//CreationTime     int64                   `json:"creation_time"`
	//ModificationTime int64                   `json:"modification_time"`
	//Notifications    []JugglerNotification   `json:"notifications"`
}

type jugglerEvent struct {
	Host        string
	Service     string
	Description string
	Level       int
}

/*
type JugglerNotification struct {
	TemplateName   string                 `json:"template_name"`
	TemplateKWArgs map[string]interface{} `json:"template_kwargs"`
	Description    string                 `json:"description"`
}
*/
