package cypress

import (
	"context"
	"testing"
	"time"

	"github.com/go-zookeeper/zk"
)

func TestZkLockRelease(t *testing.T) {
	writer := NewBufferWriter()
	SetupLogger(LogLevelDebug, writer)

	conn, _, err := zk.Connect([]string{"localhost:2181"}, time.Second*10)
	if err != nil {
		t.Error("not able to connect to local server", err)
		return
	}

	defer conn.Close()
	_, err = conn.Create("/locks1", []byte{}, 0, zk.WorldACL(zk.PermAll))
	if err != nil && err != zk.ErrNodeExists {
		t.Error("not able to create lock root", err)
		DumpBufferWriter(t, writer)
		return
	}

	defer conn.Delete("/locks1", 0)

	lock := NewZkLock(conn, "/locks1/test1")
	_, err = lock.Lock(context.Background())
	if err != nil {
		t.Error("failed to lock with single process", err)
		DumpBufferWriter(t, writer)
		return
	}

	lock.Release()

	_, err = lock.Lock(context.Background())
	if err != nil {
		t.Error("failed to lock with single process", err)
		DumpBufferWriter(t, writer)
		return
	}

	lock.Release()
}

func TestZkLockWithCancelledByTimeout(t *testing.T) {
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

	_, err = conn.Create("/locks2", []byte{}, 0, zk.WorldACL(zk.PermAll))
	if err != nil && err != zk.ErrNodeExists {
		t.Error("not able to create lock root", err)
		DumpBufferWriter(t, writer)
		return
	}

	defer conn.Delete("/locks2", 0)

	lock1 := NewZkLock(conn, "/locks2/test2")
	lock2 := NewZkLock(conn1, "/locks2/test2")
	ch := make(chan int, 1)
	go func() {
		lock1.Lock(context.Background())
		defer lock1.Release()
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

	_, err = lock2.Lock(context.Background())
	if err != nil {
		t.Error("failed to lock with background context", err)
		DumpBufferWriter(t, writer)
		return
	}

	lock2.Release()
}

func TestZkLockCancelled(t *testing.T) {
	writer := NewBufferWriter()
	SetupLogger(LogLevelDebug, writer)

	conn, _, err := zk.Connect([]string{"localhost:2181"}, time.Second*10)
	if err != nil {
		t.Error("not able to connect to local server", err)
		return
	}

	defer conn.Close()

	_, err = conn.Create("/locks3", []byte{}, 0, zk.WorldACL(zk.PermAll))
	if err != nil && err != zk.ErrNodeExists {
		t.Error("not able to create lock root", err)
		DumpBufferWriter(t, writer)
		return
	}

	defer conn.Delete("/locks3", 0)

	lock := NewZkLock(conn, "/locks3/test3")

	ctx, cancelFunc := context.WithCancel(context.Background())
	cancelFunc()
	_, err = lock.Lock(ctx)
	if err != ErrLockCancelled {
		t.Error("lock expected to be failed with ErrLockCancelled", err)
		DumpBufferWriter(t, writer)
		return
	}
}

func TestZkLockWithContention(t *testing.T) {
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

	_, err = conn.Create("/locks4", []byte{}, 0, zk.WorldACL(zk.PermAll))
	if err != nil && err != zk.ErrNodeExists {
		t.Error("not able to create lock root", err)
		DumpBufferWriter(t, writer)
		return
	}

	defer conn.Delete("/locks4", 0)

	lock1 := NewZkLock(conn, "/locks4/test4")
	lock2 := NewZkLock(conn1, "/locks4/test4")
	counter := 0
	ch := make(chan int32, 1)
	proc := func(lock *ZkLock) {
		for i := 0; i < 100; i++ {
			func() {
				_, e1 := lock.Lock(context.Background())
				if e1 != nil {
					t.Error("failed to lock with contention", e1)
					DumpBufferWriter(t, writer)
					return
				}
				defer lock.Release()
				counter++
			}()
		}

		ch <- 1
	}
	go proc(lock1)
	go proc(lock2)
	<-ch
	<-ch
	if counter != 200 {
		t.Error("counter is not increased atomically, expected 200, actuall ", counter)
		DumpBufferWriter(t, writer)
		return
	}
}

func TestZkLockWithSameInstanceContention(t *testing.T) {
	writer := NewBufferWriter()
	SetupLogger(LogLevelDebug, writer)

	conn, _, err := zk.Connect([]string{"localhost:2181"}, time.Second*10)
	if err != nil {
		t.Error("not able to connect to local server", err)
		return
	}

	defer conn.Close()

	_, err = conn.Create("/locks5", []byte{}, 0, zk.WorldACL(zk.PermAll))
	if err != nil && err != zk.ErrNodeExists {
		t.Error("not able to create lock root", err)
		DumpBufferWriter(t, writer)
		return
	}

	defer conn.Delete("/locks5", 0)

	lock := NewZkLock(conn, "/locks5/test5")
	counter := 0
	ch := make(chan int32, 1)
	proc := func(lock *ZkLock) {
		for i := 0; i < 100; i++ {
			func() {
				_, e1 := lock.Lock(context.Background())
				if e1 != nil {
					t.Error("failed to lock with contention", e1)
					DumpBufferWriter(t, writer)
					return
				}
				defer lock.Release()
				counter++
			}()
		}

		ch <- 1
	}
	go proc(lock)
	go proc(lock)
	<-ch
	<-ch
	if counter != 200 {
		t.Error("counter is not increased atomically, expected 200, actuall ", counter)
		DumpBufferWriter(t, writer)
		return
	}
}
