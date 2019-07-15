package cypress

import (
	"context"
	"sync"
	"time"
)

// Context keys
const (
	TraceActivityIDKey   = "TraceActivityID"
	UserPrincipalKey     = "UserPrincipal"
	SessionKey           = "UserSession"
	multiValueContextKey = "_multiValueContext"
)

type multiValueCtx struct {
	lock   *sync.RWMutex
	values map[string]interface{}
	parent context.Context
}

func extendContext(ctx context.Context) *multiValueCtx {
	if ctx == nil {
		panic("source context cannot be nil")
	}

	newContext := &multiValueCtx{
		lock:   &sync.RWMutex{},
		values: make(map[string]interface{}),
		parent: ctx,
	}

	newContext.withValue(multiValueContextKey, newContext)
	return newContext
}

// Deadline deadline of the context
func (ctx *multiValueCtx) Deadline() (deadline time.Time, ok bool) {
	return ctx.parent.Deadline()
}

// Done done channel
func (ctx *multiValueCtx) Done() <-chan struct{} {
	return ctx.parent.Done()
}

// Err error of context
func (ctx *multiValueCtx) Err() error {
	return ctx.parent.Err()
}

// Value value for the given key
func (ctx *multiValueCtx) Value(contextKey interface{}) interface{} {
	ctx.lock.RLock()
	defer ctx.lock.RUnlock()
	key, ok := contextKey.(string)
	if ok {
		value, ok := ctx.values[key]
		if ok {
			return value
		}
	}

	return ctx.parent.Value(contextKey)
}

func (ctx *multiValueCtx) withValue(key string, value interface{}) *multiValueCtx {
	ctx.lock.Lock()
	defer ctx.lock.Unlock()
	ctx.values[key] = value
	return ctx
}
