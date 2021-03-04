package cypress

import (
	"context"
	"database/sql"
	"os"
	"testing"

	_ "github.com/go-sql-driver/mysql"
)

func TestUniqueID(t *testing.T) {
	id, err := NewUniqueID(1, 2, 3)
	if err != nil {
		t.Error("unexpected error", err)
		return
	}

	if id.Partition() != 2 {
		t.Error("unexpected partition", id.Partition())
		return
	}

	if int32(id.Value&SegmentedIDMask) != 3 {
		t.Error("unexpected segmented id", id.Value)
		return
	}

	if id.Value>>PooledIDBitWidth != 1 {
		t.Error("unexpected pooled id", id.Value)
		return
	}
}

func TestGenerateMoreThanMaxSegmentedIdValues(t *testing.T) {
	mysqlPort := os.Getenv("MYSQL_PORT")
	if len(mysqlPort) == 0 {
		mysqlPort = "3306"
	}

	db, err := sql.Open("mysql", "root:User_123@tcp(127.0.0.1:"+mysqlPort+")/")
	if err != nil {
		t.Skip("database is not available", err)
		return
	}

	defer db.Close()
	_, err = db.Exec("create database cypress_test_uid_gen_1")
	if err != nil {
		t.Error("failed to create database", err)
		return
	}

	defer db.Exec("drop database cypress_test_uid_gen_1")
	err = func() error {
		db, err := sql.Open("mysql", "root:User_123@tcp(127.0.0.1:"+mysqlPort+")/cypress_test_uid_gen_1")
		if err != nil {
			return err
		}

		defer db.Close()
		_, err = db.Exec("create table `pooled_id`(`name` varchar(200) not null, `partition` int not null, `pooled_id` bigint not null, constraint `pk_id_generator` primary key (`name`, `partition`))")
		if err != nil {
			return err
		}

		gen := NewDbUniqueIDGenerator(NewDbAccessor(db))
		for i := 0; i < 200; i = i + 1 {
			gen.NextUniqueID(context.Background(), "testKey", 1)
		}

		id, err := gen.NextUniqueID(context.Background(), "testKey", 1)
		if err != nil {
			return err
		}

		if id.Partition() != 1 {
			t.Error("unexpected partition id", id.Partition())
			return nil
		}

		if (id.Value >> PooledIDBitWidth) != 2 {
			t.Error("unexpected pooled id", id.Value>>PooledIDBitWidth)
			return nil
		}

		if (id.Value & SegmentedIDMask) != 74 {
			t.Error("unexpected segmented id", id.Value&SegmentedIDMask)
			return nil
		}

		return nil
	}()

	if err != nil {
		t.Error("test failed with error", err)
	}
}

func TestConcurrentGeneration(t *testing.T) {
	mysqlPort := os.Getenv("MYSQL_PORT")
	if len(mysqlPort) == 0 {
		mysqlPort = "3306"
	}

	db, err := sql.Open("mysql", "root:User_123@tcp(127.0.0.1:"+mysqlPort+")/")
	if err != nil {
		t.Skip("database is not available", err)
		return
	}

	defer db.Close()
	_, err = db.Exec("create database cypress_test_uid_gen_2")
	if err != nil {
		t.Error("failed to create database", err)
		return
	}

	defer db.Exec("drop database cypress_test_uid_gen_2")
	err = func() error {
		db, err := sql.Open("mysql", "root:User_123@tcp(127.0.0.1:"+mysqlPort+")/cypress_test_uid_gen_2")
		if err != nil {
			return err
		}

		defer db.Close()
		_, err = db.Exec("create table `pooled_id`(`name` varchar(200) not null, `partition` int not null, `pooled_id` bigint not null, constraint `pk_id_generator` primary key (`name`, `partition`))")
		if err != nil {
			return err
		}

		generateIDs := func(g UniqueIDGenerator, num int) []int64 {
			results := make([]int64, 0)
			for i := 0; i < num; i = i + 1 {
				v, e := g.NextUniqueID(context.Background(), "testKey1", 1)
				if e != nil {
					t.Log("failed to generate IDs due to", e)
					i-- // retry
					continue
				}

				results = append(results, v.Value)
			}

			return results
		}

		ch := make(chan []int64)
		g1 := NewDbUniqueIDGenerator(NewDbAccessor(db))
		g2 := NewDbUniqueIDGenerator(NewDbAccessor(db))
		go func() {
			ch <- generateIDs(g1, 500)
		}()
		go func() {
			ch <- generateIDs(g2, 500)
		}()
		go func() {
			ch <- generateIDs(g1, 500)
		}()
		go func() {
			ch <- generateIDs(g2, 500)
		}()

		results := make(map[int64]bool)
		numOfResults := 4
		for numOfResults > 0 {
			select {
			case values := <-ch:
				for _, v := range values {
					results[v] = true
				}

				numOfResults--
				break
			}
		}

		if len(results) != 2000 {
			t.Error("expect to get 2000 unique ids but only get", len(results))
		}

		return nil
	}()

	if err != nil {
		t.Error("test failed with error", err)
	}
}
