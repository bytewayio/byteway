package cypress

// Comparer interface for comparing objects
type Comparer[T any] interface {
	Compare(lh T, rh T) int
}

// CompareFunc function implements Comparer
type CompareFunc[T any] func(T, T) int

// Compare implements Comparer
func (f CompareFunc[T]) Compare(lh T, rh T) int {
	return f(lh, rh)
}

// PageMerger a tool to merge multiple paged query results into one page
type PageMerger[T any] struct {
	pageSize int
	comparer Comparer[T]
}

// NewPageMerger create a new page merger
func NewPageMerger[T any](pageSize int, comparer Comparer[T]) *PageMerger[T] {
	return &PageMerger[T]{
		pageSize: pageSize,
		comparer: comparer,
	}
}

// Merge merge lists into a single sorted list and only return the top `pageSize` items of all lists
func (m *PageMerger[T]) Merge(lists ...[]T) []T {
	emulators := make([][]T, 0)
	results := make([]T, 0)
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
			var entity T
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
