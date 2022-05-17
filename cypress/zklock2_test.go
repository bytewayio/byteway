package cypress

import (
	"context"
	"testing"
	"time"

	"github.com/go-zookeeper/zk"
)

func TestZkLock2Release(t *testing.T) {
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

	lock, err := NewZkLock2(conn, "/locks1/test1")
	if err != nil {
		t.Error("failed to create lock node", err)
		DumpBufferWriter(t, writer)
		return
	}

	defer conn.Delete("/locks1/test1", 0)
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

func TestZkLock2WithCancelledByTimeout(t *testing.T) {
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

	lock1, err := NewZkLock2(conn, "/locks2/test2")
	if err != nil {
		t.Error("failed to create lock", err)
		DumpBufferWriter(t, writer)
		return
	}

	lock2, err := NewZkLock2(conn1, "/locks2/test2")
	if err != nil {
		t.Error("failed to create lock", err)
		DumpBufferWriter(t, writer)
		return
	}

	defer conn.Delete("/locks2/test2", 0)
	ch := make(chan int, 1)
	go func() {
		lockCtx, err := lock1.Lock(context.Background())
		if err != nil {
			t.Error("failed to acquire lock")
			DumpBufferWriter(t, writer)
			return
		}

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

func TestZkLock2Cancelled(t *testing.T) {
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

	lock, err := NewZkLock2(conn, "/locks3/test3")
	if err != nil {
		t.Error("failed to create lock", err)
		DumpBufferWriter(t, writer)
		return
	}

	defer conn.Delete("/locks3/test3", 0)

	ctx, cancelFunc := context.WithCancel(context.Background())
	cancelFunc()
	lockCtx, err := lock.Lock(ctx)
	if err != ErrLockCancelled {
		t.Error("lock expected to be failed with ErrLockCancelled", err)
		DumpBufferWriter(t, writer)
		return
	}

	lock.Unlock(lockCtx)
}

func TestZkLock2WithContention(t *testing.T) {
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

	lock1, err := NewZkLock2(conn, "/locks4/test4")
	if err != nil {
		t.Error("failed to create lock", err)
		DumpBufferWriter(t, writer)
		return
	}

	lock2, err := NewZkLock2(conn1, "/locks4/test4")
	if err != nil {
		t.Error("failed to create lock", err)
		DumpBufferWriter(t, writer)
		return
	}

	defer conn.Delete("/locks4/test4", 0)

	counter := 0
	ch := make(chan int32, 1)
	proc := func(lock *ZkLock2) {
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

func TestZkLock2WithSameInstanceContention(t *testing.T) {
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

	lock, err := NewZkLock2(conn, "/locks5/test5")
	if err != nil {
		t.Error("failed to create lock", err)
		DumpBufferWriter(t, writer)
		return
	}

	defer conn.Delete("/locks5/test5", 0)
	counter := 0
	ch := make(chan int32, 1)
	proc := func(lock *ZkLock2) {
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

func TestFindLeastSeqAndPrevNode(t *testing.T) {
	nodes := []string{"node-0000000000", "node-0000000005"}

	// no prevNode
	seq, node, err := findLeastSeqAndPrevNode(nodes, "node-", "", 0)
	if err != nil {
		t.Error("unexpected error", err)
		return
	}

	if seq != 0 {
		t.Error("0 expected, but found", seq)
		return
	}

	if len(node) != 0 {
		t.Error("empty expected, but found", node)
		return
	}

	// prevNode is the first node
	seq, node, err = findLeastSeqAndPrevNode(nodes, "node-", "", 5)
	if err != nil {
		t.Error("unexpected error", err)
		return
	}

	if seq != 0 {
		t.Error("0 expected, but found", seq)
		return
	}

	if node != nodes[0] {
		t.Error(nodes[0]+" expected, but found", node)
		return
	}

	// prevNode in the middle
	nodes = []string{"node-0000000000", "node-0000000005", "node-0000000006"}
	seq, node, err = findLeastSeqAndPrevNode(nodes, "node-", "", 6)
	if err != nil {
		t.Error("unexpected error", err)
		return
	}

	if seq != 0 {
		t.Error("0 expected, but found", seq)
		return
	}

	if node != nodes[1] {
		t.Error(nodes[1]+" expected, but found", node)
		return
	}
}

func TestFindLeastSeqAndPrevNodeWithOverflow(t *testing.T) {
	nodes := []string{"node-2147483643", "node-2147483645", "node--2147483647", "node--2147483646"}
	seq, node, err := findLeastSeqAndPrevNode(nodes, "node-", "", 2147483643)
	if err != nil {
		t.Error("unexpected error", err)
		return
	}

	if seq != 2147483643 {
		t.Error("2147483643 expected, but found", seq)
		return
	}

	if len(node) != 0 {
		t.Error("empty is expected, but found", node)
		return
	}

	nodes = []string{"node-2147483643", "node-2147483645", "node--2147483647", "node--2147483646"}
	seq, node, err = findLeastSeqAndPrevNode(nodes, "node-", "", -2147483647)
	if err != nil {
		t.Error("unexpected error", err)
		return
	}

	if seq != 2147483643 {
		t.Error("2147483643 expected, but found", seq)
		return
	}

	if node != nodes[1] {
		t.Error(nodes[1]+" is expected, but found", node)
		return
	}

	nodes = []string{"node-2147483643", "node-2147483645", "node--2147483647", "node--2147483646"}
	seq, node, err = findLeastSeqAndPrevNode(nodes, "node-", "", -2147483646)
	if err != nil {
		t.Error("unexpected error", err)
		return
	}

	if seq != 2147483643 {
		t.Error("2147483643 expected, but found", seq)
		return
	}

	if node != nodes[2] {
		t.Error(nodes[2]+" is expected, but found", node)
		return
	}

	nodes = []string{"node-2147483643", "node-2147483645", "node--2147483647", "node--2147483646"}
	seq, node, err = findLeastSeqAndPrevNode(nodes, "node-", "", 2147483645)
	if err != nil {
		t.Error("unexpected error", err)
		return
	}

	if seq != 2147483643 {
		t.Error("2147483643 expected, but found", seq)
		return
	}

	if node != nodes[0] {
		t.Error(nodes[0]+" is expected, but found", node)
		return
	}
}

func TestFindLeastSeqAndPrevNodeWithMultiPrefixesAndOverflow(t *testing.T) {
	nodes := []string{"read-2147483643", "read-2147483645", "write--2147483647", "read--2147483646"}
	seq, node, err := findLeastSeqAndPrevNode(nodes, "read-", "write-", 2147483643)
	if err != nil {
		t.Error("unexpected error", err)
		return
	}

	if seq != 2147483643 {
		t.Error("2147483643 expected, but found", seq)
		return
	}

	if len(node) != 0 {
		t.Error("empty is expected, but found", node)
		return
	}

	nodes = []string{"read-2147483643", "read-2147483645", "write--2147483647", "read--2147483646"}
	seq, node, err = findLeastSeqAndPrevNode(nodes, "read-", "write-", -2147483647)
	if err != nil {
		t.Error("unexpected error", err)
		return
	}

	if seq != 2147483643 {
		t.Error("2147483643 expected, but found", seq)
		return
	}

	if node != nodes[1] {
		t.Error(nodes[1]+" is expected, but found", node)
		return
	}

	nodes = []string{"read-2147483643", "read-2147483645", "write--2147483647", "read--2147483646"}
	seq, node, err = findLeastSeqAndPrevNode(nodes, "read-", "write-", -2147483646)
	if err != nil {
		t.Error("unexpected error", err)
		return
	}

	if seq != 2147483643 {
		t.Error("2147483643 expected, but found", seq)
		return
	}

	if node != nodes[2] {
		t.Error(nodes[2]+" is expected, but found", node)
		return
	}

	nodes = []string{"read-2147483643", "read-2147483645", "write--2147483647", "read--2147483646"}
	seq, node, err = findLeastSeqAndPrevNode(nodes, "read-", "write-", 2147483645)
	if err != nil {
		t.Error("unexpected error", err)
		return
	}

	if seq != 2147483643 {
		t.Error("2147483643 expected, but found", seq)
		return
	}

	if node != nodes[0] {
		t.Error(nodes[0]+" is expected, but found", node)
		return
	}
}

func TestFindLeastSeqAndPrevNodeForRWLock(t *testing.T) {
	nodes := []string{"reader-0000000000", "reader-0000000005"}

	// no prevNode
	seq, node, err := findLeastSeqAndPrevNode(nodes, "writer-", "", 0)
	if err != nil {
		t.Error("unexpected error", err)
		return
	}

	if seq != 0 {
		t.Error("0 expected, but found", seq)
		return
	}

	if len(node) != 0 {
		t.Error("empty expected, but found", node)
		return
	}

	// prevNode is the first node
	seq, node, err = findLeastSeqAndPrevNode(nodes, "writer-", "", 5)
	if err != nil {
		t.Error("unexpected error", err)
		return
	}

	if seq != 5 {
		t.Error("5 expected, but found", seq)
		return
	}

	if len(node) != 0 {
		t.Error("empty expected, but found", node)
		return
	}

	// prevNode in the middle
	nodes = []string{"reader-0000000000", "writer-0000000005", "reader-0000000006"}
	seq, node, err = findLeastSeqAndPrevNode(nodes, "writer-", "", 6)
	if err != nil {
		t.Error("unexpected error", err)
		return
	}

	if seq != 5 {
		t.Error("0 expected, but found", seq)
		return
	}

	if node != nodes[1] {
		t.Error(nodes[1]+" expected, but found", node)
		return
	}

	seq, node, err = findLeastSeqAndPrevNode(nodes, "writer-", "", 0)
	if err != nil {
		t.Error("unexpected error", err)
		return
	}

	if seq != 0 {
		t.Error("0 expected, but found", seq)
		return
	}

	if len(node) != 0 {
		t.Error("empty expected, but found", node)
		return
	}
}

func TestFindLeastSeqAndPrevNodeForRWLockWithOverflow(t *testing.T) {
	nodes := []string{"reader-2147483643", "reader-2147483645", "writer--2147483647", "writer--2147483646"}
	seq, node, err := findLeastSeqAndPrevNode(nodes, "writer-", "", 2147483643)
	if err != nil {
		t.Error("unexpected error", err)
		return
	}

	if seq != 2147483643 {
		t.Error("2147483643 expected, but found", seq)
		return
	}

	if len(node) != 0 {
		t.Error("empty is expected, but found", node)
		return
	}

	nodes = []string{"reader-2147483643", "reader-2147483645", "writer--2147483647", "writer--2147483646"}
	seq, node, err = findLeastSeqAndPrevNode(nodes, "reader-", "writer-", -2147483647)
	if err != nil {
		t.Error("unexpected error", err)
		return
	}

	if seq != 2147483643 {
		t.Error("2147483643 expected, but found", seq)
		return
	}

	if node != nodes[1] {
		t.Error(nodes[1]+" is expected, but found", node)
		return
	}

	nodes = []string{"reader-2147483643", "writer-2147483645", "reader--2147483647", "writer--2147483646"}
	seq, node, err = findLeastSeqAndPrevNode(nodes, "reader-", "writer-", -2147483646)
	if err != nil {
		t.Error("unexpected error", err)
		return
	}

	if seq != 2147483643 {
		t.Error("2147483643 expected, but found", seq)
		return
	}

	if node != nodes[2] {
		t.Error(nodes[2]+" is expected, but found", node)
		return
	}

	nodes = []string{"writer-2147483643", "writer-2147483645", "reader--2147483647", "reader--2147483646"}
	seq, node, err = findLeastSeqAndPrevNode(nodes, "writer-", "", -2147483646)
	if err != nil {
		t.Error("unexpected error", err)
		return
	}

	if seq != 2147483643 {
		t.Error("2147483643 expected, but found", seq)
		return
	}

	if node != nodes[1] {
		t.Error(nodes[1]+" is expected, but found", node)
		return
	}

	seq, node, err = findLeastSeqAndPrevNode(nodes, "writer-", "reader-", 2147483643)
	if err != nil {
		t.Error("unexpected error", err)
		return
	}

	if seq != 2147483643 {
		t.Error("2147483643 expected, but found", seq)
		return
	}

	if len(node) != 0 {
		t.Error("empty is expected, but found", node)
		return
	}

	nodes = []string{"reader-2147483643", "writer-2147483645", "reader--2147483647", "reader--2147483646"}
	seq, node, err = findLeastSeqAndPrevNode(nodes, "writer-", "", -2147483646)
	if err != nil {
		t.Error("unexpected error", err)
		return
	}

	if seq != 2147483645 {
		t.Error("2147483645 expected, but found", seq)
		return
	}

	if node != nodes[1] {
		t.Error(nodes[1]+" is expected, but found", node)
		return
	}
}
