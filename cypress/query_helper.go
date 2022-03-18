package cypress

import (
	"context"
	"errors"
	"reflect"
)

func ClusterQueryOne[T any](ctx context.Context, cluster *MyCluster, query string, mapper RowMapper, aggregator func(*T, *T) *T, args ...interface{}) (*T, error) {
	result, err := cluster.QueryOne(
		ctx,
		query,
		mapper,
		func(left any, right any) any {
			t1 := left.(*T)
			t2 := right.(*T)
			return aggregator(t1, t2)
		},
		args...)
	if err != nil {
		return nil, err
	}

	entity, ok := result.(*T)
	if !ok {
		return nil, errors.New("unexpected error: not able to convert entity to target type")
	}

	return entity, nil
}

func ClusterGetOne[T any](ctx context.Context, cluster *MyCluster, query string, aggregator func(*T, *T) *T, args ...interface{}) (*T, error) {
	ty := reflect.TypeOf((*T)(nil)).Elem()
	return ClusterQueryOne(ctx, cluster, query, NewSmartMapper(ty), aggregator, args...)
}

func ClusterQueryAll[T any](ctx context.Context, cluster *MyCluster, query string, mapper RowMapper, args ...interface{}) ([][]*T, error) {
	results, err := cluster.QueryAll(ctx, query, mapper, args...)
	if err != nil {
		return nil, err
	}

	items := make([][]*T, 0, len(results))
	for _, list := range results {
		if len(list) > 0 {
			newList := make([]*T, 0, len(list))
			for _, item := range list {
				if t, ok := item.(*T); ok {
					newList = append(newList, t)
				}
			}

			items = append(items, newList)
		}
	}

	return items, nil
}

func ClusterGetAll[T any](ctx context.Context, cluster *MyCluster, query string, args ...interface{}) ([][]*T, error) {
	return ClusterQueryAll[T](ctx, cluster, query, NewSmartMapper(reflect.TypeOf((*T)(nil)).Elem()), args...)
}

func ClusterQueryPage[T any](ctx context.Context, cluster *MyCluster, query string, mapper RowMapper, merger *PageMerger[any], args ...interface{}) ([]*T, error) {
	results, err := cluster.QueryPage(ctx, query, mapper, merger, args...)
	if err != nil {
		return nil, err
	}

	items := make([]*T, 0, len(results))
	for _, item := range results {
		if entity, ok := item.(*T); ok {
			items = append(items, entity)
		}
	}

	return items, nil
}

func ClusterGetPage[T any](ctx context.Context, cluster *MyCluster, query string, merger *PageMerger[any], args ...interface{}) ([]*T, error) {
	return ClusterQueryPage[T](ctx, cluster, query, NewSmartMapper(reflect.TypeOf((*T)(nil))), merger, args...)
}

func TxnGetOneByKey[T any](txn Txn, key interface{}) (*T, error) {
	item, err := txn.GetOneByKey(reflect.TypeOf((*T)(nil)).Elem(), key)
	if err != nil {
		return nil, err
	}

	if t, ok := item.(*T); ok {
		return t, nil
	}

	return nil, errors.New("unexpected error: failed to covert entity to target type")
}

func TxnGetOne[T any](txn Txn, proto *T) (*T, error) {
	item, err := txn.GetOne(proto)
	if err != nil {
		return nil, err
	}

	if t, ok := item.(*T); ok {
		return t, nil
	}

	return nil, errors.New("unexpected error: failed to covert entity to target type")
}

func TxnQueryOne[T any](txn Txn, sql string, mapper RowMapper, args ...interface{}) (*T, error) {
	item, err := txn.QueryOne(sql, mapper, args...)
	if err != nil {
		return nil, err
	}

	if t, ok := item.(*T); ok {
		return t, nil
	}

	return nil, errors.New("unexpected error: failed to covert entity to target type")
}

func TxnQueryAll[T any](txn Txn, sql string, mapper RowMapper, args ...interface{}) ([]*T, error) {
	results, err := txn.QueryAll(sql, mapper, args...)
	if err != nil {
		return nil, err
	}

	items := make([]*T, 0, len(results))
	for _, item := range results {
		if entity, ok := item.(*T); ok {
			items = append(items, entity)
		}
	}

	return items, nil
}

func AccessorGetOneByKey[T any](ctx context.Context, accessor *DbAccessor, key interface{}) (*T, error) {
	item, err := accessor.GetOneByKey(ctx, reflect.TypeOf((*T)(nil)).Elem(), key)
	if err != nil {
		return nil, err
	}

	if t, ok := item.(*T); ok {
		return t, nil
	}

	return nil, errors.New("unexpected error: failed to covert entity to target type")
}

func AccessorGetOne[T any](ctx context.Context, accessor *DbAccessor, proto *T) (*T, error) {
	item, err := accessor.GetOne(ctx, proto)
	if err != nil {
		return nil, err
	}

	if t, ok := item.(*T); ok {
		return t, nil
	}

	return nil, errors.New("unexpected error: failed to covert entity to target type")
}

func AccessorQueryOne[T any](ctx context.Context, accessor *DbAccessor, sql string, mapper RowMapper, args ...interface{}) (*T, error) {
	item, err := accessor.QueryOne(ctx, sql, mapper, args...)
	if err != nil {
		return nil, err
	}

	if t, ok := item.(*T); ok {
		return t, nil
	}

	return nil, errors.New("unexpected error: failed to covert entity to target type")
}

func AccessorQueryAll[T any](ctx context.Context, accessor *DbAccessor, sql string, mapper RowMapper, args ...interface{}) ([]*T, error) {
	results, err := accessor.QueryAll(ctx, sql, mapper, args...)
	if err != nil {
		return nil, err
	}

	items := make([]*T, 0, len(results))
	for _, item := range results {
		if entity, ok := item.(*T); ok {
			items = append(items, entity)
		}
	}

	return items, nil
}
