package common

import (
	"io"
	"net"
	"syscall"
)

type Temporary interface {
	Temporary() bool
}

// errors that originate from user functions/containers
type FunctionOrigin interface {
	FunctionOrigin() bool
}

func IsTemporary(err error) bool {
	v, ok := err.(Temporary)
	return (ok && v.Temporary()) || isNet(err)
}

func isNet(err error) bool {
	if _, ok := err.(net.Error); ok {
		return true
	}

	switch err := err.(type) {
	case *net.OpError:
		return true
	case syscall.Errno:
		if err == syscall.ECONNREFUSED { // linux only? maybe ok for prod
			return true // connection refused
		}
	default:
		if err == io.ErrUnexpectedEOF || err == io.EOF {
			return true
		}
	}
	return false
}

// returns true if the error originated from user functions/containers
func IsFunctionOrigin(err error) bool {
	v, ok := err.(FunctionOrigin)
	return (ok && v.FunctionOrigin())
}

// implement common.IsFunctionOrigin()
type functionOrigin struct {
	error
}

func (t *functionOrigin) IsFunctionOrigin() bool { return true }

func newFunctionOrigin(err error) error {
	return &functionOrigin{err}
}
