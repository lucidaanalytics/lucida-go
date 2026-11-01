package lucida

import (
	"errors"
	"fmt"
)

var (
	errNoDistinctID    = errors.New("lucida: event dropped: no distinct id (pass one, put one in WithRequestContext, or set Personless)")
	errBeforeSendPanic = errors.New("lucida: BeforeSend panicked; the original event was kept")
)

// DeliveryError is passed to Config.OnError when a batch is given up on.
type DeliveryError struct {
	Status  int // last HTTP status, 0 for a network error
	Dropped int // events lost
}

func (e *DeliveryError) Error() string {
	return fmt.Sprintf("lucida: dropped %d events (status %d)", e.Dropped, e.Status)
}
