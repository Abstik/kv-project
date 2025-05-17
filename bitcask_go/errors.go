package bitcask_go

import "errors"

var (
	ErrKeyIsEmpty        = errors.New("the key is Empty")
	ErrIndexUpdateFailed = errors.New("failed to update index")
	ErrKeyNotFound       = errors.New("the key is not found")
	ErrDataFileNotFound  = errors.New("data file is not found")
)
