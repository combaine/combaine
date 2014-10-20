
import msgpack

# // type ParsingTask struct {
# //    CommonTask
# //    // Hostname of target
# //    Host string
# //    // Name of handled parsing config
# //    ParsingConfigName string
# //    // Content of the current parsing config
# //    ParsingConfig configs.ParsingConfig
# //    // Content of aggreagtion configs
# //    // related to the current parsing config
# //    AggregationConfigs map[string]configs.AggregationConfig
# // }


class ParsingTask(object):
    def __init__(self, packed_task):
        self.task = msgpack.unpackb(packed_task)

    def Host(self):
        return self.task["Host"]
