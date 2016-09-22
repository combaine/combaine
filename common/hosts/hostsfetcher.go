package hosts

import "os"

type Hosts map[string][]string

func (h *Hosts) getHosts(bool remote) []string {
	hosts := make([]string, 0)
	myname := os.Hostname()
	for _, hostsInDc := range *h {
		if remote {
			for _, host := range hostsInDc {
				if myname != host {
					hosts = append(hosts, host)
				}
			}
		} else {
			hosts = append(hosts, hostsInDc...)
		}
	}
	return hosts
}

func (h *Hosts) AllHosts() []string {
	return h.getHosts(false)
}

func (h *Hosts) RemoteHosts() []string {
	return h.getHosts(true)
}
func (h *Hosts) Merge(other *Hosts) {
	for dc, v := range *other {
		(*h)[dc] = append((*h)[dc], v...)
	}
}
