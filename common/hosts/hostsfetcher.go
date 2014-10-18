package hosts

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
)

// ToDo: implement as interface

/*
host1 dc1
host2 dc2
host3 dc1
host4 dc4
*/

type HostsMap map[string][]string

type HostsFetcher interface {
	Hosts() (hosts []string, err error)
	HostsBySubgroups() (HostsMap, error)
}

func HTTPFetcher(handle string, groupname string) (h HostsFetcher, err error) {
	url := fmt.Sprintf("%s%s", handle, groupname)
	resp, err := http.Get(url)
	if err != nil {
		return
	}

	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return
	}

	var f fetcher = body
	h = &f

	return
}

type fetcher []byte

func (f *fetcher) Hosts() (hosts []string, err error) {
	s := strings.TrimSuffix(string(*f), "\n")
	for _, dcAndHost := range strings.Split(s, "\n") {
		if temp := strings.Split(dcAndHost, "\t"); len(temp) == 2 {
			hosts = append(hosts, temp[1])
		} else {
			err = fmt.Errorf("Wrong input string %s", temp)
			return
		}
	}

	if len(hosts) == 0 {
		err = fmt.Errorf("No hosts")
	}
	return
}

func (f *fetcher) HostsBySubgroups() (hm HostsMap, err error) {
	hm = make(HostsMap)
	s := strings.TrimSuffix(string(*f), "\n")
	for _, dcAndHost := range strings.Split(s, "\n") {
		if temp := strings.Split(dcAndHost, "\t"); len(temp) == 2 {
			dc := temp[0]
			host := temp[1]

			if _, ok := hm[dc]; !ok {
				hm[dc] = []string{host}
			} else {
				hm[dc] = append(hm[dc], host)
			}

		} else {
			err = fmt.Errorf("Wrong input string %s", temp)
			return
		}
	}

	if len(hm) == 0 {
		err = fmt.Errorf("No hosts")
	}
	return
}
