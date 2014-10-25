package tasks

type FetcherTask struct {
	CommonTask
	Target string `codec:"Target"`
}
