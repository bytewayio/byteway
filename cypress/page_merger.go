package cypress

// Comparer interface for comparing objects
type Comparer interface {
	Compare(lh interface{}, rh interface{}) int
}

// CompareFunc function implements Comparer
type CompareFunc func(interface{}, interface{}) int

// Compare implements Comparer
func (f CompareFunc) Compare(lh interface{}, rh interface{}) int {
	return f(lh, rh)
}

// PageMerger a tool to merge multiple paged query results into one page
type PageMerger struct {
	pageSize int
	comparer Comparer
}

// NewPageMerger create a new page merger
func NewPageMerger(pageSize int, comparer Comparer) *PageMerger {
	return &PageMerger{
		pageSize: pageSize,
		comparer: comparer,
	}
}

// Merge merge lists into a single sorted list and only return the top `pageSize` items of all lists
func (m *PageMerger) Merge(lists ...[]interface{}) []interface{} {
	emulators := make([][]interface{}, 0)
	results := make([]interface{}, 0)
	if len(lists) > 0 {
		for _, l := range lists {
			if len(l) > 0 {
				emulators = append(emulators, l)
			}
		}

		indexes := make([]int, len(emulators))
		status := make([]int, len(emulators))

		for i := 0; i < len(status); i++ {
			status[i] = 1   // need to move next
			indexes[i] = -1 //bol
		}

		for len(results) < m.pageSize {
			var entity interface{} = nil
			idx := -1
			for i, l := range emulators {
				if (status[i] & 1) != 0 {
					if indexes[i] >= len(l) {
						status[i] = 2 // reach eof
					} else {
						indexes[i]++
						status[i] = 0 //moved and not eol
					}
				}

				if (status[i] & 2) == 0 {
					item := l[indexes[i]]
					if idx == -1 || m.comparer.Compare(item, entity) > 0 {
						idx = i
						entity = item
					}
				}
			}

			if idx < 0 {
				// no more item fouund
				break
			}

			results = append(results, entity)
			status[idx] |= 1 // need to move next
		}
	}

	return results
}
