package combainer

import (
	"crypto/md5"
	"fmt"
	"math/rand"
	"strconv"
	"time"

	log "github.com/Sirupsen/logrus"

	"github.com/combaine/combaine/common/configs"
)

const (
	CONFIGS_PARSING_PATH     = "/etc/combaine/parsing/"
	CONFIGS_AGGREGATION_PATH = "/etc/combaine/aggregate"
	COMBAINER_PATH           = "/etc/combaine/combaine.yaml"
)

// GenerateSessionId = uuid.New
func GenerateSessionId() string {
	var buf = make([]byte, 0, 16)
	buf = strconv.AppendInt(buf, time.Now().UnixNano(), 10)
	buf = strconv.AppendInt(buf, rand.Int63(), 10)
	val := md5.Sum(buf)
	return fmt.Sprintf("%x", string(val[:]))
}

func GenerateSessionTimeFrame(sessionDuration uint) (time.Duration, time.Duration) {
	parsingTime := time.Duration(float64(sessionDuration)*0.8) * time.Second
	wholeTime := time.Duration(sessionDuration) * time.Second
	return parsingTime, wholeTime
}

func GetAggregationConfigs(repo configs.Repository, parsingConfig *configs.ParsingConfig) (*map[string]configs.AggregationConfig, error) {
	aggregationConfigs := make(map[string]configs.AggregationConfig)
	for _, name := range parsingConfig.AggConfigs {
		content, err := repo.GetAggregationConfig(name)
		if err != nil {
			// It seems better to throw error here instead of
			// going data processing on without config
			log.Errorf("Unable to read aggregation config %s, %s", name, err)
			return nil, err
		}

		if len(parsingConfig.Placeholders) > 0 {
			content, err = content.Generate(&parsingConfig.Placeholders)
			if err != nil {
				return nil, err
			}
		}

		var aggConfig configs.AggregationConfig
		if err := content.Decode(&aggConfig); err != nil {
			log.Errorf("Unable to decode aggConfig: %s", err)
			return nil, err
		}
		aggregationConfigs[name] = aggConfig
	}

	return &aggregationConfigs, nil
}
