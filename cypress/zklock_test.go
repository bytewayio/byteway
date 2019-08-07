package cypress

import (
	"testing"
	"time"

	"github.com/samuel/go-zookeeper/zk"
)

func TestZkLockRelease(t *testing.T) {
	writer := NewBufferWriter()
	SetupLogger(LogLevelDebug, writer)

	conn, _, err := zk.Connect([]string{"localhost:2181"}, time.Second*10)
	if err != nil {
		t.Error("not able to connect to local server", err)
		return
	}

	_, err = conn.Create("/locks", []byte{}, 0, zk.WorldACL(zk.PermAll))
	if err != nil {
		t.Error("not able to create lock root", err)
		DumpBufferWriter(t, writer)
		return
	}

	defer conn.Delete("/locks", 0)

	lock := NewZkLock(conn, "/locks/test1")
	err = lock.Lock()
	if err != nil {
		t.Error("failed to lock with single process", err)
		DumpBufferWriter(t, writer)
		return
	}

	lock.Release()

	err = lock.Lock()
	if err != nil {
		t.Error("failed to lock with single process", err)
		DumpBufferWriter(t, writer)
		return
	}

	lock.Release()
}

func TestZkLockWithContention(t *testing.T) {
	writer := NewBufferWriter()
	SetupLogger(LogLevelDebug, writer)

	conn, _, err := zk.Connect([]string{"localhost:2181"}, time.Second*10)
	if err != nil {
		t.Error("not able to connect to local server", err)
		return
	}

	conn1, _, err := zk.Connect([]string{"localhost:2181"}, time.Second*10)
	if err != nil {
		t.Error("not able to connect to local server", err)
		return
	}

	_, err = conn.Create("/locks", []byte{}, 0, zk.WorldACL(zk.PermAll))
	if err != nil {
		t.Error("not able to create lock root", err)
		DumpBufferWriter(t, writer)
		return
	}

	defer conn.Delete("/locks", 0)

	lock1 := NewZkLock(conn, "/locks/test2")
	lock2 := NewZkLock(conn1, "/locks/test2")
	counter := 0
	ch := make(chan int32, 1)
	proc := func(lock *ZkLock) {
		for i := 0; i < 100; i++ {
			func() {
				e1 := lock.Lock()
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
