package hosts

import "github.com/combaine/combaine/utils"

// Hosts represent map of the DC to list of the hosts
type Hosts map[string][]string

func (h *Hosts) getHosts(remote bool) []string {
	hosts := make([]string, 0)
	myname := utils.Hostname()
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

// AllHosts return all hosts in given map
func (h *Hosts) AllHosts() []string {
	return h.getHosts(false)
}

// RemoteHosts return hosts without my hostname
func (h *Hosts) RemoteHosts() []string {
	return h.getHosts(true)
}

// Merge all hosts in all datacenters
func (h *Hosts) Merge(other *Hosts) {
	for dc, v := range *other {
		(*h)[dc] = append((*h)[dc], v...)
	}
}
