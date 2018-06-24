// Package chttp is combaine http client
package chttp

// Do sends an HTTP request and return HTTP response.
import (
	"context"
	"io"
	"net/http"
)

const defaultUserAgent = "Combaine (github.com/combaine)"

// Do general http request with Context
func Do(ctx context.Context, req *http.Request) (*http.Response, error) {
	if _, ok := req.Header["User-Agent"]; !ok {
		req.Header.Set("User-Agent", defaultUserAgent)
	}
	resp, err := http.DefaultClient.Do(req.WithContext(ctx))
	if err != nil {
		select {
		case <-ctx.Done():
			err = ctx.Err()
		default:
		}
	}
	return resp, err
}

// Get issues a GET request via the Do function.
func Get(ctx context.Context, url string) (*http.Response, error) {
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}
	return Do(ctx, req)
}

// Post issues a POST request via the Do function.
func Post(ctx context.Context, url string, bodyType string, body io.Reader) (*http.Response, error) {
	req, err := http.NewRequest("POST", url, body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", bodyType)
	return Do(ctx, req)
}
