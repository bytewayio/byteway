package cypress

import (
	"reflect"
	"strings"
)

var (
	//EntityToTableNameMapper entity name to table name mapper
	EntityToTableNameMapper = TableNameResolverFunc(ToSnakeCase)

	// SQLIdentifierQuoteLeft sql identifier quote left
	SQLIdentifierQuoteLeft = "`"

	// SQLIdentifierQuoteRight sql identifier quote right
	SQLIdentifierQuoteRight = "`"

	entityDescriptorsCache = NewConcurrentMapTypeEnforced(reflect.TypeOf(&EntityDescriptor{}))
)

func quoteIdentifier(name string) string {
	return SQLIdentifierQuoteLeft + name + SQLIdentifierQuoteRight
}

type fieldInfo struct {
	field  *reflect.StructField
	column string
}

type keyInfo struct {
	field   *fieldInfo
	autoGen bool
}

// EntityDescriptor entity descriptor
type EntityDescriptor struct {
	entityType   reflect.Type
	tableName    string
	key          *keyInfo
	multiKeys    []*fieldInfo
	partitionKey *fieldInfo
	fields       []*fieldInfo
	insertFields []*fieldInfo
	updateFields []*fieldInfo
	InsertSQL    string
	UpdateSQL    string
	DeleteSQL    string
	GetOneSQL    string
}

func newEntityDescriptor(entityType reflect.Type) *EntityDescriptor {
	return &EntityDescriptor{
		entityType:   entityType,
		multiKeys:    make([]*fieldInfo, 0),
		fields:       make([]*fieldInfo, 0, 10),
		insertFields: make([]*fieldInfo, 0, 10),
		updateFields: make([]*fieldInfo, 0, 10),
	}
}

// GetInsertValues get insert values for the given entity
func (descriptor *EntityDescriptor) GetInsertValues(entity interface{}) []interface{} {
	if entity == nil {
		panic("not able to get insert values for nil entity")
	}

	value := reflect.ValueOf(entity)
	if value.Kind() == reflect.Ptr {
		value = value.Elem()
	}

	result := make([]interface{}, len(descriptor.insertFields))
	for index, field := range descriptor.insertFields {
		result[index] = value.FieldByIndex(field.field.Index).Interface()
	}

	return result
}

// GetUpdateValues get update values for the given entity
func (descriptor *EntityDescriptor) GetUpdateValues(entity interface{}) []interface{} {
	if entity == nil {
		panic("not able to get update values for nil entity")
	}

	value := reflect.ValueOf(entity)
	if value.Kind() == reflect.Ptr {
		value = value.Elem()
	}

	result := make([]interface{}, len(descriptor.updateFields))
	for index, field := range descriptor.updateFields {
		result[index] = value.FieldByIndex(field.field.Index).Interface()
	}

	return result
}

// GetKeyValues get key value for the given entity
func (descriptor *EntityDescriptor) GetKeyValues(entity interface{}) []interface{} {
	if entity == nil {
		panic("not able to get key value for nil entity")
	}

	if (descriptor.key == nil || descriptor.key.field == nil) && len(descriptor.multiKeys) == 0 {
		return nil
	}

	value := reflect.ValueOf(entity)
	if value.Kind() == reflect.Ptr {
		value = value.Elem()
	}

	if descriptor.key != nil {
		keyValue := value.FieldByIndex(descriptor.key.field.field.Index).Interface()
		return []interface{}{keyValue}
	}

	keyValues := make([]interface{}, 0, len(descriptor.multiKeys))
	for _, k := range descriptor.multiKeys {
		keyValues = append(keyValues, value.FieldByIndex(k.field.Index).Interface())
	}

	return keyValues
}

func createEntityDescriptor(entityType reflect.Type) *EntityDescriptor {
	ty := entityType
	for ty.Kind() == reflect.Ptr {
		ty = ty.Elem()
	}

	descriptor := newEntityDescriptor(ty)
	descriptor.tableName = EntityToTableNameMapper.Resolve(ty.Name())
	for i := 0; i < ty.NumField(); i = i + 1 {
		field := ty.Field(i)
		tags := strings.Split(field.Tag.Get(EntityTagsDTags), ",")
		tagsSet := make(map[string]bool)
		for t := 0; t < len(tags); t = t + 1 {
			tagsSet[strings.ToLower(strings.Trim(tags[t], " \t"))] = true
		}

		hasTag := func(tag string) bool {
			v, ok := tagsSet[tag]
			return v && ok
		}

		// valid tags: alias, noupdate, key, nogen, autoinc, partition,multikey
		alias := hasTag(EntityTagsAlias)
		name := field.Tag.Get(EntityTagsAlias)
		if name == "" {
			name = field.Tag.Get(EntityTagsCol)
		} else {
			alias = true // declared as alias
		}

		if name == "" {
			name = ToSnakeCase(field.Name)
		}

		if !alias {
			fi := &fieldInfo{
				field:  &field,
				column: name,
			}

			descriptor.fields = append(descriptor.fields, fi)
			if hasTag(EntityTagsDTagsKey) {
				if descriptor.key != nil {
					panic("duplicate key declared for " + ty.Name())
				}

				descriptor.key = &keyInfo{
					field:   fi,
					autoGen: !hasTag(EntityTagsDTagsNoGen),
				}
			} else if hasTag(EntityTagsDTagsMultiKey) {
				descriptor.multiKeys = append(descriptor.multiKeys, fi)
			} else if !hasTag(EntityTagsDTagsNoUpdate) && !hasTag(EntityTagsDTagsPartition) {
				descriptor.updateFields = append(descriptor.updateFields, fi)
			}

			if hasTag(EntityTagsDTagsPartition) {
				descriptor.partitionKey = fi
			}

			if !hasTag(EntityTagsDTagsAutoInc) {
				descriptor.insertFields = append(descriptor.insertFields, fi)
			}
		}
	}

	if descriptor.key != nil || len(descriptor.multiKeys) > 0 {
		whereClause := " where "
		if descriptor.key != nil {
			whereClause += quoteIdentifier(descriptor.key.field.column) + "=?"
		} else {
			conditions := make([]string, 0, len(descriptor.multiKeys))
			for _, k := range descriptor.multiKeys {
				conditions = append(conditions, quoteIdentifier(k.column)+"=?")
			}

			whereClause += strings.Join(conditions, " and ")
		}

		descriptor.DeleteSQL = "delete from " + quoteIdentifier(descriptor.tableName) + whereClause
		first := true
		descriptor.UpdateSQL = "update " + quoteIdentifier(descriptor.tableName) + " set "
		for _, fi := range descriptor.updateFields {
			if !first {
				descriptor.UpdateSQL += ", "
			} else {
				first = false
			}

			descriptor.UpdateSQL += quoteIdentifier(fi.column) + "=?"
		}

		descriptor.UpdateSQL += whereClause
		descriptor.GetOneSQL = "select * from " + quoteIdentifier(descriptor.tableName) + whereClause
	}

	params := ""
	first := true
	descriptor.InsertSQL = "insert into " + quoteIdentifier(descriptor.tableName) + "("
	for _, fi := range descriptor.insertFields {
		if !first {
			descriptor.InsertSQL += ","
			params += ","
		} else {
			first = false
		}

		descriptor.InsertSQL += quoteIdentifier(fi.column)
		params += "?"
	}

	descriptor.InsertSQL += ") values(" + params + ")"
	return descriptor
}

// GetOrCreateEntityDescriptor get or create EntityDescriptor for the given type
func GetOrCreateEntityDescriptor(entityType reflect.Type) *EntityDescriptor {
	ty := entityType
	for ty.Kind() == reflect.Ptr {
		ty = entityType.Elem()
	}

	item := entityDescriptorsCache.GetOrCompute(entityType.PkgPath()+"/"+ty.Name(), func() interface{} {
		return createEntityDescriptor(ty)
	})

	return item.(*EntityDescriptor)
}
