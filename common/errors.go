package common

import "errors"

var (
	ErrAppCall = errors.New("Application call error")
	// ErrAppUnavailable is an application for parsing/aggregating is not found
	ErrAppUnavailable = errors.New("Application is unavailable")
)
