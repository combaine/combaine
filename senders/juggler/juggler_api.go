package juggler

type JugglerResponse map[ /*hostname*/ string]map[ /*serviceName*/ string]JugglerCheck

/*
type JugglerNotification struct {
	TemplateName   string                 `json:"template_name"`
	TemplateKWArgs map[string]interface{} `json:"template_kwargs"`
	Description    string                 `json:"description"`
}
*/

type JugglerChildrenCheck struct {
	Instance string `json:"instance"`
	Host     string `json:"host"`
	Type     string `json:"type"`
	Service  string `json:"service"`
}

type JugglerAggregatorKWArgs struct {
	IgnoreNodata int64                    `json:"ignore_nodata"`
	Limits       []JugglerAggregatorLimit `json:"limits"`
}

type JugglerAggregatorLimit struct {
	Crit      int64 `json:"crit"`
	TimeStart int64 `json:"time_start"`
	TimeEnd   int64 `json:"time_end"`
	DayStart  int64 `json:"day_start"`
	DayEnd    int64 `json:"day_end"`
}

type JugglerCheck struct {
	Description      string                  `json:"description"`
	CreationTime     int64                   `json:"creation_time"`
	ModificationTime int64                   `json:"modification_time"`
	RefreshTime      int64                   `json:"refresh_time"`
	Ttl              int64                   `json:"ttl"`
	MaxStatus        string                  `json:"max_status"`
	AlertInterval    []int64                 `json:"alert_interval"`
	Aggregator       string                  `json:"aggregator"`
	AggregatorKWArgs JugglerAggregatorKWArgs `json:"aggregator_kwargs"`
	Tags             []string                `json:"tags"`
	Methods          []string                `json:"methods"`
	Active           string                  `json:"active"`
	ActiveKWArgs     map[string]string       `json:"active_kwargs"`
	FlapTime         int64                   `json:"flap_tiem"`
	StableTime       int64                   `json:"stable_time"`
	CriticalTime     int64                   `json:"critical_time"`
	BoostTime        int64                   `json:"boost_time"`
	Children         []JugglerChildrenCheck  `json:"children"`
	//Notifications    []JugglerNotification   `json:"notifications"`
}
