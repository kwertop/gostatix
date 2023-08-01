package filters

type BaseFilter[T any] interface {
	Insert(element T) (bool, error)
	Lookup(element T) (bool, error)
}
