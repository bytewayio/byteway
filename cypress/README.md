# cypress

An integrated web server and websocket framework

## features

1. pipeline based request handlers
2. security as a request handler
3. logging as a request handler
4. MVC support
5. Redis based session store support
6. context for all, supports cancellation and correlation logging
7. horizental database partitioning
8. XA transaction support

## XA transaction support requirements

- User privileges

For MySQL 8.0 [XA_RECOVER_ADMIN is required](https://dev.mysql.com/doc/refman/8.0/en/privileges-provided.html#priv_xa-recover-admin)

- Unique key generator

There is a default database based unique key generator, since the tables requires by the generator would not exceed a few hundreds of rows, there is no sharding support for this version of unique key generator. The following data table is required for DB based unique key generator.

```mysql
create table `id_generator` (
    `name` varchar(200) not null,
    `partition` int not null,
    `pooled_id` int not null,
    constraint `pk_id_generator` primary key (`name`, `partition`)
  ) engine=InnoDB default charset=utf8;
```

- Cluster transaction state store

A cluster transaction state store is required to support distributed transactions, the store is used to provide transaction states for unknown transaction state resolver to decide how to move on with outstanding but timeout transactions.

There is a DB based transaction store implementation provided, which requires the following data tables,

```mysql
  create table `cluster_txn` (
      `id` varchar(40) auto_increment not null primary key,
      `state` int not null default 0,
      `timestamp` bigint not null,
      `lease_expiration` bigint not null default 0
  ) engine=InnoDB default charset=utf8 auto_increment=10001;
  create table `txn_participant` (
      `txn_id` varchar(40) not null,
      `partition` int not null,
      `state` int not null default 0,
      constraint `pk_txn_participant` primary key (`txn_id`, `partition`)
  ) engine=InnoDB default charset=utf8;
```
