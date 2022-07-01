package cypress

import (
	"context"
	"errors"
	"math"
	"reflect"
	"strconv"
	"strings"

	"go.uber.org/zap"
)

type PageQuery[T any, TPrimaryKey any, TSecondaryKey any] struct {
	Cluster                     *MyCluster
	CountingQueryText           string
	CountingQueryArgsConfigurer func() []interface{}
	QuerySetup                  func(sql *strings.Builder) ([]interface{}, bool)
	PrimaryKeyName              string
	SecondaryKeyName            string
	PrimaryKeyParser            func(value string) (TPrimaryKey, bool)
	SecondaryKeyParser          func(value string) (TSecondaryKey, bool)
	PrimaryKeyGetter            func(t *T) TPrimaryKey
	SecondaryKeyGetter          func(t *T) TSecondaryKey
	PrimaryKeyComparer          Comparer[TPrimaryKey]
	SecondaryKeyComparer        Comparer[TSecondaryKey]
}

type DataPage[T any] struct {
	Page         int         `json:"page"`
	TotalRecords int64       `json:"totalRecords"`
	TotalPages   int         `json:"totalPages"`
	Records      []T         `json:"records"`
	PageToken    string      `json:"pageToken"`
	Summary      interface{} `json:"summary,omitempty"`
}

type Stringify interface {
	String() string
}

type pageKey[TPrimaryKey any, TSecondaryKey any] struct {
	primaryKey   TPrimaryKey
	secondaryKey TSecondaryKey
	keySeparator string
}

type keyRange[TPrimaryKey any, TSecondaryKey any] struct {
	lowerBound     *pageKey[TPrimaryKey, TSecondaryKey]
	upperBound     *pageKey[TPrimaryKey, TSecondaryKey]
	rangeSeparator string
}

type pageToken[TPrimaryKey any, TSecondaryKey any] struct {
	page           int
	keyRange       *keyRange[TPrimaryKey, TSecondaryKey]
	rangeSeparator string
}

func tryParsePageKey[TPrimaryKey any, TSecondaryKey any](
	encodedString string,
	keySeparator string,
	primaryKeyParser func(value string) (TPrimaryKey, bool),
	secondaryKeyParser func(value string) (TSecondaryKey, bool)) (*pageKey[TPrimaryKey, TSecondaryKey], bool) {
	values := strings.Split(encodedString, keySeparator)
	if len(values) != 2 {
		return nil, false
	}

	primaryKey, ok := primaryKeyParser(values[0])
	if !ok {
		return nil, false
	}

	secondaryKey, ok := secondaryKeyParser(values[1])
	if !ok {
		return nil, false
	}

	return &pageKey[TPrimaryKey, TSecondaryKey]{primaryKey: primaryKey, secondaryKey: secondaryKey, keySeparator: keySeparator}, true
}

func tryParseKeyRange[TPrimaryKey any, TSecondaryKey any](
	encodedString, rangeSeparator, keySeparator string,
	primaryKeyParser func(value string) (TPrimaryKey, bool),
	secondaryKeyParser func(value string) (TSecondaryKey, bool)) (*keyRange[TPrimaryKey, TSecondaryKey], bool) {
	values := strings.Split(encodedString, rangeSeparator)
	if len(values) != 2 {
		return nil, false
	}

	lowerBound, ok := tryParsePageKey(values[0], keySeparator, primaryKeyParser, secondaryKeyParser)
	if !ok {
		return nil, false
	}

	upperBound, ok := tryParsePageKey(values[1], keySeparator, primaryKeyParser, secondaryKeyParser)
	if !ok {
		return nil, false
	}

	return &keyRange[TPrimaryKey, TSecondaryKey]{lowerBound, upperBound, rangeSeparator}, true
}

func tryParsePageToken[TPrimaryKey any, TSecondaryKey any](
	encodedString, rangeSeparator, keySeparator string,
	primaryKeyParser func(value string) (TPrimaryKey, bool),
	secondaryKeyParser func(value string) (TSecondaryKey, bool)) (*pageToken[TPrimaryKey, TSecondaryKey], bool) {
	pos := strings.Index(encodedString, rangeSeparator)
	if pos <= 0 {
		return nil, false
	}

	pageValue := encodedString[0:pos]
	page, err := strconv.ParseInt(pageValue, 10, 64)
	if err != nil {
		zap.L().Warn("bad page value in page token", zap.String("page", pageValue), zap.Error(err))
		return nil, false
	}

	rangeValue := encodedString[pos+1:]
	keyRange, ok := tryParseKeyRange(rangeValue, rangeSeparator, keySeparator, primaryKeyParser, secondaryKeyParser)
	if !ok {
		return nil, false
	}

	return &pageToken[TPrimaryKey, TSecondaryKey]{
		page:           int(page),
		keyRange:       keyRange,
		rangeSeparator: rangeSeparator,
	}, true
}

func keyValueAsString(value interface{}) string {
	if v, ok := value.(int); ok {
		return strconv.Itoa(v)
	}

	if v, ok := value.(*int); ok {
		return strconv.Itoa(*v)
	}

	if v, ok := value.(int64); ok {
		return strconv.FormatInt(v, 10)
	}

	if v, ok := value.(*int64); ok {
		return strconv.FormatInt(*v, 10)
	}

	if v, ok := value.(int32); ok {
		return strconv.Itoa(int(v))
	}

	if v, ok := value.(*int32); ok {
		return strconv.Itoa(int(*v))
	}

	if v, ok := value.(string); ok {
		return v
	}

	if v, ok := value.(*string); ok {
		return *v
	}

	if v, ok := value.(Stringify); ok {
		return v.String()
	}

	if v, ok := value.(uint); ok {
		return strconv.FormatUint(uint64(v), 10)
	}

	if v, ok := value.(*uint); ok {
		return strconv.FormatUint(uint64(*v), 10)
	}

	if v, ok := value.(uint64); ok {
		return strconv.FormatUint(v, 10)
	}

	if v, ok := value.(*uint64); ok {
		return strconv.FormatUint(*v, 10)
	}

	if v, ok := value.(uint32); ok {
		return strconv.FormatUint(uint64(v), 10)
	}

	if v, ok := value.(*uint32); ok {
		return strconv.FormatUint(uint64(*v), 10)
	}

	if v, ok := value.(int8); ok {
		return strconv.Itoa(int(v))
	}

	if v, ok := value.(*int8); ok {
		return strconv.Itoa(int(*v))
	}

	if v, ok := value.(int16); ok {
		return strconv.Itoa(int(v))
	}

	if v, ok := value.(*int16); ok {
		return strconv.Itoa(int(*v))
	}

	if v, ok := value.(uint8); ok {
		return strconv.FormatUint(uint64(v), 10)
	}

	if v, ok := value.(*uint8); ok {
		return strconv.FormatUint(uint64(*v), 10)
	}

	if v, ok := value.(uint16); ok {
		return strconv.FormatUint(uint64(v), 10)
	}

	if v, ok := value.(*uint16); ok {
		return strconv.FormatUint(uint64(*v), 10)
	}

	panic("cannot convert value to string")
}

func (k *pageKey[TPrimaryKey, TSecondaryKey]) AsString() string {
	return keyValueAsString(k.primaryKey) + k.keySeparator + keyValueAsString(k.secondaryKey)
}

func (r *keyRange[TPrimaryKey, TSecondaryKey]) AsString() string {
	return r.lowerBound.AsString() + r.rangeSeparator + r.upperBound.AsString()
}

func (t *pageToken[TPrimaryKey, TSecondaryKey]) AsString() string {
	return strconv.Itoa(t.page) + t.rangeSeparator + t.keyRange.AsString()
}

func (q *PageQuery[T, TPrimaryKey, TSecondaryKey]) DoQuery(ctx context.Context, paging, pageToken string, pageSize int) (*DataPage[*T], error) {
	return q.DoQueryWithCustomPageToken(ctx, paging, pageToken, pageSize, ",", "-")
}

func (q *PageQuery[T, TPrimaryKey, TSecondaryKey]) DoQueryWithCustomPageToken(ctx context.Context, paging, pagingToken string, pageSize int, rangeSeparator, keySeparator string) (*DataPage[*T], error) {
	doPaging := paging
	if doPaging != PagingNext && doPaging != PagingPrev {
		doPaging = PagingNone
	}

	token, isValid := tryParsePageToken(pagingToken, rangeSeparator, keySeparator, q.PrimaryKeyParser, q.SecondaryKeyParser)
	if !isValid {
		doPaging = PagingNone
		token = &pageToken[TPrimaryKey, TSecondaryKey]{}
	}

	v, err := q.Cluster.GetOne(
		ctx,
		reflect.TypeOf(0),
		q.CountingQueryText,
		func(i1, i2 interface{}) interface{} { return i1.(int) + i2.(int) },
		q.CountingQueryArgsConfigurer()...)
	if err != nil {
		return nil, err
	}

	totalRecords := v.(int)
	totalPages := int(math.Ceil(float64(totalRecords) / float64(pageSize)))
	sql := &strings.Builder{}
	parameters, hasWhereClause := q.QuerySetup(sql)
	if doPaging == PagingPrev {
		if !hasWhereClause {
			sql.WriteString(" where ")
		} else {
			sql.WriteString(" and ")
		}

		sql.WriteString("(")
		sql.WriteString(q.PrimaryKeyName)
		sql.WriteString(">? or (")
		sql.WriteString(q.PrimaryKeyName)
		sql.WriteString("=? and ")
		sql.WriteString(q.SecondaryKeyName)
		sql.WriteString(">?)) order by ")
		sql.WriteString(q.PrimaryKeyName)
		sql.WriteString(" asc, ")
		sql.WriteString(q.SecondaryKeyName)
		sql.WriteString(" asc limit ?")
		parameters = append(
			parameters,
			token.keyRange.lowerBound.primaryKey,
			token.keyRange.lowerBound.primaryKey,
			token.keyRange.lowerBound.secondaryKey)
	} else if doPaging == PagingNext {
		if !hasWhereClause {
			sql.WriteString(" where ")
		} else {
			sql.WriteString(" and ")
		}

		sql.WriteString("(")
		sql.WriteString(q.PrimaryKeyName)
		sql.WriteString("<? or (")
		sql.WriteString(q.PrimaryKeyName)
		sql.WriteString("=? and ")
		sql.WriteString(q.SecondaryKeyName)
		sql.WriteString("<?)) order by ")
		sql.WriteString(q.PrimaryKeyName)
		sql.WriteString(" desc, ")
		sql.WriteString(q.SecondaryKeyName)
		sql.WriteString(" desc limit ?")
		parameters = append(
			parameters,
			token.keyRange.upperBound.primaryKey,
			token.keyRange.upperBound.primaryKey,
			token.keyRange.upperBound.secondaryKey)
	} else {
		sql.WriteString(" order by ")
		sql.WriteString(q.PrimaryKeyName)
		sql.WriteString(" desc, ")
		sql.WriteString(q.SecondaryKeyName)
		sql.WriteString(" desc limit ?")
	}

	parameters = append(parameters, pageSize)
	results, err := q.Cluster.GetPage(
		ctx,
		reflect.TypeOf((*T)(nil)).Elem(),
		sql.String(),
		NewPageMerger[any](pageSize, CompareFunc[any](func(t1, t2 any) int {
			if t1 == nil || t2 == nil {
				panic(errors.New("unexpected nil entity in query result"))
			}

			e1 := t1.(*T)
			e2 := t2.(*T)
			compareResult := q.PrimaryKeyComparer.Compare(q.PrimaryKeyGetter(e1), q.PrimaryKeyGetter(e2))
			if doPaging == PagingPrev {
				if compareResult == 0 {
					return q.SecondaryKeyComparer.Compare(q.SecondaryKeyGetter(e2), q.SecondaryKeyGetter(e1))
				}

				return -1 * compareResult
			}

			if compareResult == 0 {
				return q.SecondaryKeyComparer.Compare(q.SecondaryKeyGetter(e1), q.SecondaryKeyGetter(e2))
			}

			return compareResult
		})),
		parameters...)

	if err != nil {
		return nil, err
	}

	currentPage := token.page + 1
	if doPaging == PagingPrev {
		if token.page-1 > 0 {
			currentPage = token.page - 1
		} else {
			currentPage = 1
		}
	} else if doPaging == PagingPrev {
		if isValid {
			currentPage = token.page
		} else {
			currentPage = 1
		}
	}

	if len(results) == 0 {
		return &DataPage[*T]{
			Records:      []*T{},
			TotalRecords: int64(totalRecords),
			TotalPages:   totalPages,
			Page:         currentPage,
			PageToken:    "",
		}, nil
	}

	records := make([]*T, len(results))
	if doPaging == PagingPrev {
		// reverse the results
		for i, j := 0, len(results)-1; i < len(results); {
			records[i] = results[j].(*T)
			i += 1
			j -= 1
		}
	} else {
		for i := 0; i < len(results); i += 1 {
			records[i] = results[i].(*T)
		}
	}

	last := records[len(records)-1]
	first := records[0]
	newRange := &keyRange[TPrimaryKey, TSecondaryKey]{
		lowerBound: &pageKey[TPrimaryKey, TSecondaryKey]{
			primaryKey:   q.PrimaryKeyGetter(first),
			secondaryKey: q.SecondaryKeyGetter(first),
			keySeparator: keySeparator,
		},
		upperBound: &pageKey[TPrimaryKey, TSecondaryKey]{
			primaryKey:   q.PrimaryKeyGetter(last),
			secondaryKey: q.SecondaryKeyGetter(last),
			keySeparator: keySeparator,
		},
		rangeSeparator: rangeSeparator,
	}

	newToken := &pageToken[TPrimaryKey, TSecondaryKey]{
		page:           currentPage,
		keyRange:       newRange,
		rangeSeparator: rangeSeparator,
	}

	return &DataPage[*T]{
		Records:      records,
		TotalRecords: int64(totalRecords),
		TotalPages:   totalPages,
		Page:         currentPage,
		PageToken:    newToken.AsString(),
	}, nil
}
