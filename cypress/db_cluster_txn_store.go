package cypress

import (
	"context"
	"errors"
	"time"
)

var (
	protoClusterTxn     = &ClusterTxn{}
	protoTxnParticipant = &TxnParticipant{}
)

// DbClusterTxnStore an database based cluster transaction store implementation
type DbClusterTxnStore struct {
	db *DbAccessor
}

// NewDbClusterTxnStore create a new database based cluster transaction store
func NewDbClusterTxnStore(db *DbAccessor) *DbClusterTxnStore {
	return &DbClusterTxnStore{db}
}

// CreateTxn create a transaction
func (store *DbClusterTxnStore) CreateTxn(ctx context.Context, txnID string, t time.Time) (*ClusterTxn, error) {
	txn := &ClusterTxn{txnID, ClusterTxnStateNone, ConvertToEpochMillis(t), 0}
	_, err := store.db.Insert(ctx, txn)
	return txn, err
}

// GetTxn get transaction by ID
func (store *DbClusterTxnStore) GetTxn(ctx context.Context, txnID string) (*ClusterTxn, error) {
	obj, err := store.db.GetOne(ctx, &ClusterTxn{ID: txnID})
	if err != nil {
		return nil, err
	}

	if obj == nil {
		return nil, nil
	}

	return obj.(*ClusterTxn), err
}

// UpdateTxnState update transaction state
func (store *DbClusterTxnStore) UpdateTxnState(ctx context.Context, txnID string, state int) error {
	result, err := store.db.Execute(ctx, "update cluster_txn set state=? where id=?", state, txnID)
	if err != nil {
		return err
	}

	if rowsUpdated, err := result.RowsAffected(); err != nil || rowsUpdated != 1 {
		if err != nil {
			return err
		}

		return errors.New("transaction not found")
	}

	return nil
}

// AddTxnParticipant add a participant to a transaction
func (store *DbClusterTxnStore) AddTxnParticipant(ctx context.Context, txnID string, partition int) error {
	participant := &TxnParticipant{txnID, partition, ClusterTxnStateNone}
	_, err := store.db.Insert(ctx, participant)
	return err
}

// DeleteTxn delete partition from store
func (store *DbClusterTxnStore) DeleteTxn(ctx context.Context, txnID string) error {
	txn, err := store.db.BeginTxn(ctx)
	if err != nil {
		return err
	}

	defer txn.Close()
	result, err := txn.Delete(&ClusterTxn{ID: txnID})
	if err != nil {
		return err
	}

	if rowsUpdated, err := result.RowsAffected(); err != nil || rowsUpdated != 1 {
		txn.Rollback()
		if err != nil {
			return err
		}

		return errors.New("transaction not found")
	}

	_, err = txn.Execute("delete from txn_participant where txn_id=?", txnID)
	if err != nil {
		txn.Rollback()
		return err
	}

	txn.Commit()
	return nil
}

// LeaseExpiredTxns lease expired transactions, only expired and lease expired (or not lease) transactions are visible
func (store *DbClusterTxnStore) LeaseExpiredTxns(ctx context.Context, expireTime, leaseTimeout time.Time, items int) ([]*ClusterTxn, error) {
	results := make([]*ClusterTxn, 0)
	tsNow := GetEpochMillis()
	tsExpiration := ConvertToEpochMillis(expireTime)
	tsLeaseTimeout := ConvertToEpochMillis(leaseTimeout)
	done := false

	for len(results) < items && !done {
		if err := func() error {
			txn, err := store.db.BeginTxn(ctx)
			if err != nil {
				return err
			}

			defer txn.Close()
			rows, err := txn.QueryAll(
				"select * from cluster_txn where `timestamp`<=? and `lease_expiration`<=? limit ?",
				NewSmartMapper(protoClusterTxn),
				tsExpiration,
				tsNow,
				items)
			if err != nil {
				return err
			}

			if len(rows) == 0 {
				done = true
				return nil
			}

			for _, row := range rows {
				entity := row.(*ClusterTxn)
				result, err := txn.Execute(
					"update cluster_txn set lease_expiration=? where id=? and lease_expiration=?",
					tsLeaseTimeout,
					entity.ID,
					entity.LeaseExpiration)
				if err != nil {
					txn.Rollback()
					return err
				}

				rowsUpdated, err := result.RowsAffected()
				if err != nil {
					txn.Rollback()
					return err
				}

				if rowsUpdated == 1 {
					results = append(results, entity)
				}
			}

			txn.Commit()
			return nil
		}(); err != nil {
			return results, err
		}
	}

	return results, nil
}

// ListParticipants list all participants in transaction
func (store *DbClusterTxnStore) ListParticipants(ctx context.Context, txnID string) ([]*TxnParticipant, error) {
	rows, err := store.db.QueryAll(
		ctx,
		"select * from txn_participant where txn_id=?",
		NewSmartMapper(protoTxnParticipant),
		txnID)
	if err != nil {
		return nil, err
	}

	results := make([]*TxnParticipant, len(rows))
	for i, row := range rows {
		results[i] = row.(*TxnParticipant)
	}

	return results, nil
}

// Close close all resources used by store
func (store *DbClusterTxnStore) Close() error {
	return store.db.db.Close()
}
