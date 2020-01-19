package cypress

import "reflect"

import "strings"

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
	partitionKey *fieldInfo
	fields       []*fieldInfo
	insertFields []*fieldInfo
	updateFields []*fieldInfo
	InsertSQL    string
	UpdateSQL    string
	DeleteSQL    string
}

func newEntityDescriptor(entityType reflect.Type) *EntityDescriptor {
	return &EntityDescriptor{
		entityType:   entityType,
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
	result := make([]interface{}, len(descriptor.updateFields))
	for index, field := range descriptor.updateFields {
		result[index] = value.FieldByIndex(field.field.Index).Interface()
	}

	return result
}

// GetKeyValue get key value for the given entity
func (descriptor *EntityDescriptor) GetKeyValue(entity interface{}) interface{} {
	if entity == nil {
		panic("not able to get key value for nil entity")
	}

	if descriptor.key == nil || descriptor.key.field == nil {
		return nil
	}

	return reflect.ValueOf(entity).FieldByIndex(descriptor.key.field.field.Index).Interface()
}

func createEntityDescriptor(entityType reflect.Type) *EntityDescriptor {
	ty := entityType
	if ty.Kind() == reflect.Ptr {
		ty = ty.Elem()
	}

	descriptor := newEntityDescriptor(ty)
	descriptor.tableName = EntityToTableNameMapper.Resolve(ty.Name())
	for i := 0; i < ty.NumField(); i = i + 1 {
		field := ty.Field(i)
		tags := strings.Split(field.Tag.Get("dtags"), ",")
		for t := 0; t < len(tags); t = t + 1 {
			tags[t] = strings.ToLower(strings.Trim(tags[t], " \t"))
		}

		hasTag := func(tag string) bool {
			for _, t := range tags {
				if t == tag {
					return true
				}
			}

			return false
		}

		// valid tags: alias, noupdate, key, nogen, autoinc, partition
		alias := hasTag("alias")
		name := field.Tag.Get("alias")
		if name == "" {
			name = field.Tag.Get("col")
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
			if hasTag("key") {
				if descriptor.key != nil {
					panic("duplicate key declared for " + ty.Name())
				}

				descriptor.key = &keyInfo{
					field:   fi,
					autoGen: !hasTag("nogen"),
				}
			} else if !hasTag("noupdate") {
				descriptor.updateFields = append(descriptor.updateFields, fi)
			}

			if hasTag("partition") {
				descriptor.partitionKey = fi
			}

			if !hasTag("autoinc") {
				descriptor.insertFields = append(descriptor.insertFields, fi)
			}
		}
	}

	if descriptor.key != nil {
		descriptor.DeleteSQL = "delete from " + quoteIdentifier(descriptor.tableName) +
			" where " + quoteIdentifier(descriptor.key.field.column) + "=?"
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

		descriptor.UpdateSQL += " where " + quoteIdentifier(descriptor.key.field.column) + "=?"
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
	item := entityDescriptorsCache.GetOrCompute(entityType.PkgPath()+"/"+entityType.Name(), func() interface{} {
		return createEntityDescriptor(entityType)
	})

	return item.(*EntityDescriptor)
}
