package cypress

import (
	"context"
	"time"

	"github.com/go-redis/redis/v8"
)

type redisSessionStore struct {
	redisDb *redis.Client
}

// NewRedisSessionStore creates a new redis based session store
func NewRedisSessionStore(cli *redis.Client) SessionStore {
	return &redisSessionStore{cli}
}

// Close closes the store
func (store *redisSessionStore) Close() {
	store.redisDb.Close()
}

// Save implements SessionStore's Save api, store the session data into redis
func (store *redisSessionStore) Save(session *Session, timeout time.Duration) error {
	ctx, cancelFunc := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancelFunc()
	if !session.IsValid {
		status := store.redisDb.Del(ctx, session.ID)
		return status.Err()
	}

	data := session.Serialize()
	status := store.redisDb.Set(ctx, session.ID, data, timeout)
	return status.Err()
}

// Get implements SessionStore's Get api, retrieves session from redis by given id
func (store *redisSessionStore) Get(id string) (*Session, error) {
	ctx, cancelFunc := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancelFunc()
	status := store.redisDb.Get(ctx, id)
	if status.Err() != nil {
		return nil, ErrSessionNotFound
	}

	data, err := status.Bytes()
	if err != nil {
		return nil, ErrSessionNotFound
	}

	session := NewSession(id)
	session.Deserialize(data)
	return session, nil
}
