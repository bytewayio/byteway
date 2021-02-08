package cypress

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/gofrs/uuid"
)

const (
	masterDb     = "ClusterTransactionTestsMaster"
	masterTables = `
	create table pooled_id (
		name varchar(200) not null,
		` + "`" + `partition` + "`" + ` int not null,
		pooled_id int not null,
		constraint pk_pooled_id primary key (name, ` + "`" + `partition` + "`" + `)
	  ) engine=InnoDB default charset=utf8;
	  create table cluster_txn (
		  id varchar(40) not null primary key,
		  state int not null default 0,
		  ` + "`" + `timestamp` + "`" + ` bigint not null,
		  lease_expiration bigint not null default 0
	  ) engine=InnoDB default charset=utf8;
	  create table txn_participant (
		  txn_id varchar(40) not null,
		  ` + "`" + `partition` + "`" + ` int not null,
		  state int not null default 0,
		  constraint pk_txn_participant primary key (txn_id, ` + "`" + `partition` + "`" + `)
	  ) engine=InnoDB default charset=utf8;
	`
	partition1      = "ClusterTransactionTestsP0"
	partition2      = "ClusterTransactionTestsP1"
	partitionTables = `
	create table balance (
		id bigint not null primary key,
		account varchar(100) not null,
		balance int not null default 0
	) engine=InnoDB;
	create table sub_balance (
		id bigint not null primary key,
		balance_id bigint not null,
		adjustment int not null default 0
	) engine=InnoDB;
	create table no_key_entity (
		id varchar(40) not null primary key,
		value varchar(100) not null
	) engine=InnoDB;
	create table multi_key_entity (
		key1 varchar(40) not null,
		key2 varchar(40) not null,
		value varchar(100) not null,
		primary key (key1, key2)
	) engine=InnoDB;`
)

func runClusterTest(t *testing.T, runner func(*MyCluster) error) {
	SetupLogger(LogLevelDebug, NewRollingLogWriter("test.log", 1, 10))
	db, err := sql.Open("mysql", "root:User_123@tcp(127.0.0.1:3306)/")
	if err != nil {
		t.Skip("Skip database related tests as dev env is not configured", err)
		return
	}

	defer db.Close()
	_, err = db.Exec("create database " + masterDb)
	if err != nil {
		t.Error("Not able to create master db")
		return
	}

	defer db.Exec("drop database " + masterDb)

	_, err = db.Exec("create database " + partition1)
	if err != nil {
		t.Error("Not able to create partition1 db")
		return
	}

	defer db.Exec("drop database " + partition1)

	_, err = db.Exec("create database " + partition2)
	if err != nil {
		t.Error("Not able to create partition2 db")
		return
	}

	defer db.Exec("drop database " + partition2)

	master, err := sql.Open("mysql", "root:User_123@tcp(127.0.0.1:3306)/"+masterDb)
	if err != nil {
		t.Error("Failed to open master db", err)
		return
	}

	tables := strings.Split(masterTables, ";")
	for _, s := range tables {
		if len(strings.Trim(s, " \t\r\n")) > 0 {
			_, err = master.Exec(s)
			if err != nil {
				t.Error("Failed to setup master db", err)
				return
			}
		}
	}

	partition1Db, err := sql.Open("mysql", "root:User_123@tcp(127.0.0.1:3306)/"+partition1)
	if err != nil {
		t.Error("Failed to open partition1 db", err)
		return
	}

	tables = strings.Split(partitionTables, ";")
	for _, s := range tables {
		if len(strings.Trim(s, " \t\r\n")) > 0 {
			_, err = partition1Db.Exec(s)
			if err != nil {
				t.Error("Failed to setup partition1 db", err)
				return
			}
		}
	}

	partition2Db, err := sql.Open("mysql", "root:User_123@tcp(127.0.0.1:3306)/"+partition2)
	if err != nil {
		t.Error("Failed to open partition2 db", err)
		return
	}

	for _, s := range tables {
		if len(strings.Trim(s, " \t\r\n")) > 0 {
			_, err = partition2Db.Exec(s)
			if err != nil {
				t.Error("Failed to setup partition2 db", err)
				return
			}
		}
	}

	masterAccessor := NewDbAccessor(master)
	partitionAccessors := make([]*DbAccessor, 2)
	partitionAccessors[0] = NewDbAccessor(partition1Db)
	partitionAccessors[1] = NewDbAccessor(partition2Db)
	cluster := NewMyCluster(masterAccessor, partitionAccessors, 5, NewDbUniqueIDGenerator(masterAccessor), PartitionCalculateFunc(CalculateMd5PartitionKey))
	defer cluster.Close()
	err = runner(cluster)
	if err != nil {
		t.Error("Test failed due to error", err)
	}
}

type balance struct {
	ID          int64  `col:"id" dtags:"key,partition"`
	AccountName string `col:"account"`
	Amount      int    `col:"balance"`
}

type subBalance struct {
	ID         int64 `col:"id" dtags:"key"`
	BalanceID  int64 `col:"balance_id" dtags:"partition"`
	Adjustment int   `col:"adjustment"`
}

type noKeyEntity struct {
	Key      string `col:"id" dtags:"key,nogen"`
	Value    string `col:"value"`
	NonExist string `alias:"no_exist"`
}

type multiKeyEntity struct {
	Key1  string `col:"key1" dtags:"multikey"`
	Key2  string `col:"key2" dtags:"multikey,partition"`
	Value string `col:"value"`
}

func TestMyClusterInsert(t *testing.T) {
	runClusterTest(t, func(cluster *MyCluster) error {
		b := &balance{
			AccountName: "test2",
			Amount:      200,
		}

		err := cluster.Insert(context.Background(), b)
		if err != nil {
			return err
		}

		if b.ID <= 0 {
			t.Error("Balance ID is not assigned")
		}

		return nil
	})
}

func TestClusterDeleteByKey(t *testing.T) {
	runClusterTest(t, func(cluster *MyCluster) error {
		b := &balance{
			AccountName: "test3",
			Amount:      200,
		}

		err := cluster.InsertAt(context.Background(), 8, b)
		if err != nil {
			return err
		}

		if b.ID < 0 {
			t.Error("Balance ID is not assigned")
			return nil
		}

		_, err = cluster.Delete(context.Background(), b)
		if err != nil {
			return err
		}

		item, err := cluster.GetDbAccessorByID(b.ID).GetOne(context.Background(), b)
		if err != nil {
			return err
		}

		if item != nil {
			t.Error("unexpected entity from db")
		}

		return nil
	})
}

func TestClusterUpdate(t *testing.T) {
	runClusterTest(t, func(cluster *MyCluster) error {
		b := &balance{
			AccountName: "test1",
			Amount:      100,
		}

		err := cluster.Insert(context.Background(), b)
		if err != nil {
			return err
		}

		item, err := cluster.GetDbAccessorByID(b.ID).GetOne(context.Background(), b)
		if err != nil {
			return err
		}

		if item.(*balance).Amount != 100 {
			t.Error("unexpected data after insert")
			return nil
		}

		b = item.(*balance)
		b.Amount = 200
		_, err = cluster.GetDbAccessorByID(b.ID).Update(context.Background(), b)
		if err != nil {
			return err
		}

		item, err = cluster.GetDbAccessorByID(b.ID).GetOne(context.Background(), b)
		if err != nil {
			return err
		}

		if item.(*balance).Amount != 200 {
			t.Error("unexpected data after insert")
			return nil
		}

		return nil
	})
}

func TestClusterNonAutoGenInsert(t *testing.T) {
	runClusterTest(t, func(cluster *MyCluster) error {
		entity := &noKeyEntity{
			Key:   "some random text",
			Value: "value1",
		}

		err := cluster.InsertAt(context.Background(), 7, entity)
		if err != nil {
			return err
		}

		item, err := cluster.GetDbAccessor(7).GetOne(context.Background(), entity)
		if err != nil {
			return err
		}

		if item.(*noKeyEntity).Value != "value1" {
			t.Error("unexpected value")
		}

		return nil
	})
}

func TestClusterNonAutoGenUpdate(t *testing.T) {
	runClusterTest(t, func(cluster *MyCluster) error {
		entity := &noKeyEntity{
			Key:   "some random text",
			Value: "value1",
		}

		err := cluster.InsertAt(context.Background(), 7, entity)
		if err != nil {
			return err
		}

		item, err := cluster.GetDbAccessor(7).GetOne(context.Background(), entity)
		if err != nil {
			return err
		}

		entity = item.(*noKeyEntity)
		entity.Value = "value2"
		err = cluster.UpdateAt(context.Background(), 7, entity)
		if err != nil {
			return err
		}

		item, err = cluster.GetDbAccessor(7).GetOne(context.Background(), entity)
		if err != nil {
			return err
		}

		if item.(*noKeyEntity).Value != "value2" {
			t.Error("unexpected value")
		}

		return nil
	})
}

func TestClusterGetAll(t *testing.T) {
	runClusterTest(t, func(cluster *MyCluster) error {
		b := &balance{
			AccountName: "test1",
			Amount:      100,
		}

		err := cluster.Insert(context.Background(), b)
		if err != nil {
			return err
		}

		b = &balance{
			AccountName: "test2",
			Amount:      200,
		}

		err = cluster.Insert(context.Background(), b)
		if err != nil {
			return err
		}

		err = cluster.Insert(context.Background(), &subBalance{
			BalanceID:  b.ID,
			Adjustment: 100,
		})

		if err != nil {
			return err
		}

		all, err := cluster.GetAll(context.Background(), b, "select * from `balance`")
		if err != nil {
			return err
		}

		if len(all) != 2 {
			t.Error("unexpected number of partitions", len(all))
			return nil
		}

		data, err := cluster.GetDbAccessorByID(b.ID).QueryOne(context.Background(), "select * from `sub_balance` where `balance_id`=?", NewSmartMapper(&subBalance{}), b.ID)
		if err != nil {
			return err
		}

		if data.(*subBalance).Adjustment != 100 {
			t.Error("unexpected data after insert")
		}

		return nil
	})
}

func TestClusterTxnWithSuccess(t *testing.T) {
	runClusterTest(t, func(cluster *MyCluster) error {
		err := func() error {
			b1 := &balance{
				AccountName: "test1",
				Amount:      100,
			}
			b2 := &balance{
				AccountName: "test3",
				Amount:      200,
			}

			txn, err := cluster.CreateTransaction(context.Background())
			if err != nil {
				return err
			}

			defer txn.Close()
			_, err = txn.InsertAt(0, b1)
			if err != nil {
				return err
			}

			_, err = txn.InsertAt(1, b2)
			if err != nil {
				return err
			}

			txn.Commit()
			return nil
		}()

		if err != nil {
			return err
		}

		all, err := cluster.GetAll(context.Background(), &balance{}, "select * from `balance`")
		if err != nil {
			return err
		}

		if len(all) != 2 {
			t.Error("unexpected number of partitions", len(all))
			return nil
		}

		count := 0
		for _, l := range all {
			count += len(l)
		}

		if count != 2 {
			t.Error("unexpected number of entities", count)
		}

		return nil
	})
}

func TestClusterTxnWithFailure(t *testing.T) {
	runClusterTest(t, func(cluster *MyCluster) error {
		err := func() error {
			b1 := &balance{
				AccountName: "test1",
				Amount:      100,
			}
			b2 := &balance{
				AccountName: "test3",
				Amount:      200,
			}

			txn, err := cluster.CreateTransaction(context.Background())
			if err != nil {
				return err
			}

			defer txn.Close()
			_, err = txn.InsertAt(0, b1)
			if err != nil {
				return err
			}

			_, err = txn.InsertAt(1, b2)
			if err != nil {
				return err
			}

			t, err := txn.GetTxnByPartition(GetPartitionKey(b1.ID))
			if err != nil {
				return err
			}

			b, err := t.GetOneByKey(b1, b1.ID)
			if err != nil {
				return err
			}

			if b == nil {
				return errors.New("object not found after insert")
			}

			return nil
		}()

		if err != nil {
			return err
		}

		all, err := cluster.GetAll(context.Background(), &balance{}, "select * from `balance`")
		if err != nil {
			return err
		}

		if len(all) != 2 {
			t.Error("unexpected number of partitions", len(all))
			return nil
		}

		count := 0
		for _, l := range all {
			count += len(l)
		}

		if count != 0 {
			t.Error("unexpected number of entities", count)
		}

		return nil
	})
}

func TestMultiKeyCURD(t *testing.T) {
	runClusterTest(t, func(cluster *MyCluster) error {
		entity := &multiKeyEntity{
			Key1:  "key1",
			Key2:  "key2",
			Value: "value",
		}
		err := cluster.Insert(context.Background(), entity)
		if err != nil {
			t.Error("failed to insert entity", err)
			return err
		}

		all, err := cluster.GetDbAccessorByKey("key2").QueryAll(context.Background(), "select * from multi_key_entity", NewSmartMapper(&multiKeyEntity{}))
		if err != nil {
			return err
		}

		if len(all) != 1 {
			t.Error("unexpected number of rows", len(all))
			return errors.New("unexpected number of rows")
		}

		entity = all[0].(*multiKeyEntity)
		entity.Value = "value1"
		_, err = cluster.GetDbAccessorByKey("key2").Update(context.Background(), entity)
		if err != nil {
			return err
		}

		value, err := cluster.GetDbAccessorByKey("key2").GetOne(context.Background(), &multiKeyEntity{Key1: "key1", Key2: "key2"})
		if err != nil {
			return err
		}

		if value == nil {
			return errors.New("unexpected nil value")
		}

		entity = value.(*multiKeyEntity)
		if entity.Value != "value1" {
			return errors.New("Unexpected value " + entity.Value)
		}

		_, err = cluster.GetDbAccessorByKey("key2").Delete(context.Background(), entity)
		if err != nil {
			return err
		}

		all, err = cluster.GetDbAccessorByKey("key2").QueryAll(context.Background(), "select * from multi_key_entity", NewSmartMapper(&multiKeyEntity{}))
		if err != nil {
			return err
		}

		if len(all) != 0 {
			t.Error("unexpected number of rows", len(all))
			return errors.New("unexpected number of rows")
		}

		return nil
	})
}

func TestUnknownStateClusterTxnResolution(t *testing.T) {
	runClusterTest(t, func(cluster *MyCluster) error {
		uid1, err := cluster.idGen.NextUniqueID(context.Background(), "balance", 8)
		if err != nil {
			return err
		}

		uid2, err := cluster.idGen.NextUniqueID(context.Background(), "balance", 17)
		if err != nil {
			return err
		}

		id1 := uid1.Value
		id2 := uid2.Value
		_, err = cluster.partitions[0].Execute(context.Background(), "insert into `balance`(`id`, `account`, `balance`) values(?, ?, ?)", id1, "test1", 1000)
		if err != nil {
			return err
		}

		_, err = cluster.partitions[1].Execute(context.Background(), "insert into `balance`(`id`, `account`, `balance`) values(?, ?, ?)", id2, "test2", 1000)
		if err != nil {
			return err
		}

		uid, err := uuid.NewV4()
		if err != nil {
			return err
		}

		txnID := uid.String()
		txn, err := cluster.txnStore.CreateTxn(context.Background(), txnID, time.Now())
		if err != nil {
			return err
		}

		if err = cluster.txnStore.AddTxnParticipant(context.Background(), txnID, 0); err != nil {
			return err
		}

		if err = cluster.txnStore.AddTxnParticipant(context.Background(), txnID, 1); err != nil {
			return err
		}

		gtrid1 := fmt.Sprintf("'%v','%v'", txn.ID, 0)
		gtrid2 := fmt.Sprintf("'%v','%v'", txn.ID, 1)
		err = func() error {
			conn1, err := cluster.partitions[0].db.Conn(context.Background())
			if err != nil {
				return err
			}

			defer conn1.Close()
			conn2, err := cluster.partitions[1].db.Conn(context.Background())
			if err != nil {
				return err
			}

			defer conn2.Close()
			if _, err = conn1.ExecContext(context.Background(), "XA START "+gtrid1); err != nil {
				return err
			}

			if _, err = conn1.ExecContext(context.Background(), "update `balance` set `balance`=`balance`-200 where `id`=?", id1); err != nil {
				return err
			}

			if _, err = conn1.ExecContext(context.Background(), "XA END "+gtrid1); err != nil {
				return err
			}

			if _, err = conn1.ExecContext(context.Background(), "XA PREPARE "+gtrid1); err != nil {
				return err
			}

			if _, err = conn2.ExecContext(context.Background(), "XA START "+gtrid2); err != nil {
				return err
			}

			if _, err = conn2.ExecContext(context.Background(), "update `balance` set `balance`=`balance`-200 where `id`=?", id2); err != nil {
				return err
			}

			if _, err = conn2.ExecContext(context.Background(), "XA END "+gtrid2); err != nil {
				return err
			}

			if _, err = conn2.ExecContext(context.Background(), "XA PREPARE "+gtrid2); err != nil {
				return err
			}
			return nil
		}()
		if err != nil {
			return err
		}

		time.Sleep(time.Second * 7)

		// The following execution need to wait for the pending xa transaction to be resolved
		if _, err = cluster.partitions[0].Execute(context.Background(), "update `balance` set `balance`=`balance`-200 where `id`=?", id1); err != nil {
			return err
		}

		if _, err = cluster.partitions[1].Execute(context.Background(), "update `balance` set `balance`=`balance`+200 where `id`=?", id2); err != nil {
			return err
		}

		b1, err := cluster.partitions[0].QueryOne(context.Background(), "select * from `balance` where id=?", NewSmartMapper(&balance{}), id1)
		if err != nil {
			return err
		}

		if b1.(*balance).Amount != 800 {
			t.Error("expected b1 balance 800 but got", b1.(*balance).Amount)
			return nil
		}

		b2, err := cluster.partitions[1].QueryOne(context.Background(), "select * from `balance` where id=?", NewSmartMapper(&balance{}), id2)
		if err != nil {
			return err
		}

		if b2.(*balance).Amount != 1200 {
			t.Error("expected b1 balance 1200 but got", b2.(*balance).Amount)
			return nil
		}

		return nil
	})
}
