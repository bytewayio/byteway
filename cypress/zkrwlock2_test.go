package cypress

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-zookeeper/zk"
)

func TestZkRWLock2Release(t *testing.T) {
	writer := NewBufferWriter()
	SetupLogger(LogLevelDebug, writer)

	conn, _, err := zk.Connect([]string{"localhost:2181"}, time.Second*10)
	if err != nil {
		t.Error("not able to connect to local server", err)
		return
	}

	defer conn.Close()
	_, err = conn.Create("/rwlocks1", []byte{}, 0, zk.WorldACL(zk.PermAll))
	if err != nil && err != zk.ErrNodeExists {
		t.Error("not able to create lock root", err)
		DumpBufferWriter(t, writer)
		return
	}

	defer conn.Delete("/rwlocks1", 0)

	lock, err := NewZkRWLock2(conn, "/rwlocks1/test1")
	if err != nil {
		t.Error("failed to create rw lock", err)
		DumpBufferWriter(t, writer)
		return
	}

	defer func() {
		conn.Delete("/rwlocks1/test1", 0)
	}()
	lockCtx, err := lock.Lock(context.Background())
	if err != nil {
		t.Error("failed to lock with single process", err)
		DumpBufferWriter(t, writer)
		return
	}

	lock.Unlock(lockCtx)

	lockCtx, err = lock.Lock(context.Background())
	if err != nil {
		t.Error("failed to lock with single process", err)
		DumpBufferWriter(t, writer)
		return
	}

	lock.Unlock(lockCtx)
}

func TestZkRWLock2WithCancelledByTimeout(t *testing.T) {
	writer := NewBufferWriter()
	SetupLogger(LogLevelDebug, writer)

	conn, _, err := zk.Connect([]string{"localhost:2181"}, time.Second*10)
	if err != nil {
		t.Error("not able to connect to local server", err)
		return
	}

	defer conn.Close()

	conn1, _, err := zk.Connect([]string{"localhost:2181"}, time.Second*10)
	if err != nil {
		t.Error("not able to connect to local server", err)
		return
	}

	defer conn1.Close()

	_, err = conn.Create("/rwlocks2", []byte{}, 0, zk.WorldACL(zk.PermAll))
	if err != nil && err != zk.ErrNodeExists {
		t.Error("not able to create lock root", err)
		DumpBufferWriter(t, writer)
		return
	}

	defer conn.Delete("/rwlocks2", 0)

	lock1, err := NewZkRWLock2(conn, "/rwlocks2/test2")
	if err != nil {
		t.Error("failed to create rw lock", err)
		DumpBufferWriter(t, writer)
		return
	}

	defer func() {
		conn.Delete("/rwlocks2/test2", 0)
	}()

	lock2, err := NewZkRWLock2(conn1, "/rwlocks2/test2")
	if err != nil {
		t.Error("failed to create rw lock", err)
		DumpBufferWriter(t, writer)
		return
	}

	ch := make(chan int, 1)
	go func() {
		lockCtx, _ := lock1.Lock(context.Background())
		defer lock1.Unlock(lockCtx)
		ch <- 1
		time.Sleep(time.Second * 2)
	}()

	// wait for the lock to be acquired
	<-ch
	ctx, cancelFunc := context.WithTimeout(context.Background(), time.Second)
	defer cancelFunc()
	_, err = lock2.Lock(ctx)
	if err != ErrLockCancelled {
		t.Error("lock expected to be failed with ErrLockCancelled", err)
		DumpBufferWriter(t, writer)
		return
	}

	lockCtx, err := lock2.Lock(context.Background())
	if err != nil {
		t.Error("failed to lock with background context", err)
		DumpBufferWriter(t, writer)
		return
	}

	lock2.Unlock(lockCtx)
}

func TestZkRWLock2Cancelled(t *testing.T) {
	writer := NewBufferWriter()
	SetupLogger(LogLevelDebug, writer)

	conn, _, err := zk.Connect([]string{"localhost:2181"}, time.Second*10)
	if err != nil {
		t.Error("not able to connect to local server", err)
		return
	}

	defer conn.Close()

	_, err = conn.Create("/rwlocks3", []byte{}, 0, zk.WorldACL(zk.PermAll))
	if err != nil && err != zk.ErrNodeExists {
		t.Error("not able to create lock root", err)
		DumpBufferWriter(t, writer)
		return
	}

	defer conn.Delete("/rwlocks3", 0)

	lock, err := NewZkRWLock2(conn, "/rwlocks3/test3")
	if err != nil {
		t.Error("failed to create rw lock", err)
		DumpBufferWriter(t, writer)
		return
	}

	defer func() {
		conn.Delete("/rwlocks3/test3", 0)
	}()

	ctx, cancelFunc := context.WithCancel(context.Background())
	cancelFunc()
	_, err = lock.Lock(ctx)
	if err != ErrLockCancelled {
		t.Error("lock expected to be failed with ErrLockCancelled", err)
		DumpBufferWriter(t, writer)
		return
	}
}

func TestZkRWLock2WithContention(t *testing.T) {
	writer := NewBufferWriter()
	SetupLogger(LogLevelDebug, writer)

	conn, _, err := zk.Connect([]string{"localhost:2181"}, time.Second*10)
	if err != nil {
		t.Error("not able to connect to local server", err)
		return
	}

	defer conn.Close()

	conn1, _, err := zk.Connect([]string{"localhost:2181"}, time.Second*10)
	if err != nil {
		t.Error("not able to connect to local server", err)
		return
	}

	defer conn1.Close()

	_, err = conn.Create("/rwlocks4", []byte{}, 0, zk.WorldACL(zk.PermAll))
	if err != nil && err != zk.ErrNodeExists {
		t.Error("not able to create lock root", err)
		DumpBufferWriter(t, writer)
		return
	}

	defer conn.Delete("/rwlocks4", 0)

	lock1, err := NewZkRWLock2(conn, "/rwlocks4/test4")
	if err != nil {
		t.Error("failed to create rw lock", err)
		DumpBufferWriter(t, writer)
		return
	}

	defer func() {
		conn.Delete("/rwlocks4/test4", 0)
	}()

	lock2, err := NewZkRWLock2(conn1, "/rwlocks4/test4")
	if err != nil {
		t.Error("failed to create rw lock", err)
		DumpBufferWriter(t, writer)
		return
	}

	counter := 0
	ch := make(chan int32, 1)
	writerProc := func(lock *ZkRWLock2) {
		for i := 0; i < 100; i++ {
			func() {
				c, e1 := lock.Lock(context.Background())
				if e1 != nil {
					t.Error("failed to lock with contention", e1)
					DumpBufferWriter(t, writer)
					return
				}
				defer lock.Unlock(c)
				counter++
			}()
		}

		ch <- 1
	}

	var reads int32 = 0
	readerProc := func(lock *ZkRWLock2) {
		for i := 0; i < 100; i++ {
			func() {
				c, err := lock.RLock(context.Background())
				if err != nil {
					t.Error("failed to acquire reader lock with contention", err)
					DumpBufferWriter(t, writer)
					return
				}

				defer lock.RUnlock(c)
				atomic.AddInt32(&reads, 1)
			}()
		}

		ch <- 1
	}
	go writerProc(lock1)
	go writerProc(lock2)
	go readerProc(lock1)
	go readerProc(lock2)
	<-ch
	<-ch
	<-ch
	<-ch
	if counter != 200 {
		t.Error("counter is not increased atomically, expected 200, actuall ", counter)
		DumpBufferWriter(t, writer)
		return
	}

	if reads != 200 {
		t.Error("read counter is not increased atomically, expected 200, actuall ", reads)
		DumpBufferWriter(t, writer)
		return
	}
}

func TestZkRWLock2WithSameInstanceContention(t *testing.T) {
	writer := NewBufferWriter()
	SetupLogger(LogLevelDebug, writer)

	conn, _, err := zk.Connect([]string{"localhost:2181"}, time.Second*10)
	if err != nil {
		t.Error("not able to connect to local server", err)
		return
	}

	defer conn.Close()

	_, err = conn.Create("/rwlocks5", []byte{}, 0, zk.WorldACL(zk.PermAll))
	if err != nil && err != zk.ErrNodeExists {
		t.Error("not able to create lock root", err)
		DumpBufferWriter(t, writer)
		return
	}

	defer conn.Delete("/rwlocks5", 0)

	lock, err := NewZkRWLock2(conn, "/rwlocks5/test5")
	if err != nil {
		t.Error("failed to create lock")
		return
	}

	defer func() {
		conn.Delete("/rwlocks5/test5", 0)
	}()

	counter := 0
	ch := make(chan int32, 1)
	writerProc := func(lock *ZkRWLock2) {
		for i := 0; i < 100; i++ {
			func() {
				lockCtx, e1 := lock.Lock(context.Background())
				if e1 != nil {
					t.Error("failed to lock with contention", e1)
					DumpBufferWriter(t, writer)
					return
				}
				defer lock.Unlock(lockCtx)
				counter++
			}()
		}

		ch <- 1
	}
	var reads int32 = 0
	readerProc := func(lock *ZkRWLock2) {
		for i := 0; i < 100; i++ {
			func() {
				lockCtx, err := lock.RLock(context.Background())
				if err != nil {
					t.Error("failed to acquire reader lock with contention", err)
					DumpBufferWriter(t, writer)
					return
				}

				defer lock.RUnlock(lockCtx)
				atomic.AddInt32(&reads, 1)
			}()
		}

		ch <- 1
	}

	go writerProc(lock)
	go writerProc(lock)
	go readerProc(lock)
	go readerProc(lock)
	<-ch
	<-ch
	<-ch
	<-ch
	if counter != 200 {
		t.Error("counter is not increased atomically, expected 200, actuall ", counter)
		DumpBufferWriter(t, writer)
		return
	}

	if reads != 200 {
		t.Error("read counter is not increased atomically, expected 200, actuall ", reads)
		DumpBufferWriter(t, writer)
		return
	}
}
