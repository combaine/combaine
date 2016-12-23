package common

import "errors"

var (
	// ErrAppCall returned by cocaine in very strange case
	ErrAppCall = errors.New("Application call error")
	// ErrAppUnavailable is an application for parsing/aggregating is not found
	ErrAppUnavailable = errors.New("Application is unavailable")
	// ErrLockOwned say about config alredy created in this zk session by this server
	ErrLockOwned = errors.New("Config lock owned")
	// ErrLockByAnother say about config alredy locked by another client
	ErrLockByAnother = errors.New("Config locked by another client")
)
