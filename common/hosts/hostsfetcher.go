package hosts

type Hosts map[string][]string

func (h *Hosts) AllHosts() []string {
	hosts := make([]string, 0)
	for _, hostsInDc := range *h {
		hosts = append(hosts, hostsInDc...)
	}
	return hosts
}

func (h *Hosts) Merge(other *Hosts) {
	for dc, v := range *other {
		(*h)[dc] = append((*h)[dc], v...)
	}
}
