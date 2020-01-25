package cypress

import (
	"context"
	"database/sql"
	"strings"
	"testing"
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
		  id bigint auto_increment not null primary key,
		  state int not null default 0,
		  ` + "`" + `timestamp` + "`" + ` bigint not null
	  ) engine=InnoDB default charset=utf8 auto_increment=10001;
	  create table txn_participant (
		  txn_id bigint not null,
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
	) engine=InnoDB;`
)

func runClusterTest(t *testing.T, runner func(*MyCluster) error) {
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
	cluster := NewMyCluster(masterAccessor, partitionAccessors, 60, NewDbUniqueIDGenerator(masterAccessor), PartitionCalculateFunc(CalculateMd5PartitionKey))
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

		item, err := cluster.GetDbAccessorByID(b.ID).GetOne(context.Background(), b, b.ID)
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

		item, err := cluster.GetDbAccessorByID(b.ID).GetOne(context.Background(), b, b.ID)
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

		item, err = cluster.GetDbAccessorByID(b.ID).GetOne(context.Background(), b, b.ID)
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

		item, err := cluster.GetDbAccessor(7).GetOne(context.Background(), entity, entity.Key)
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

		item, err := cluster.GetDbAccessor(7).GetOne(context.Background(), entity, entity.Key)
		if err != nil {
			return err
		}

		entity = item.(*noKeyEntity)
		entity.Value = "value2"
		err = cluster.UpdateAt(context.Background(), 7, entity)
		if err != nil {
			return err
		}

		item, err = cluster.GetDbAccessor(7).GetOne(context.Background(), entity, entity.Key)
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

			txn.MarkAsFaulted()
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
