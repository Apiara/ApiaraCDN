package damocles

type NeedTracker interface {
	GetScore(string) (int64, error)
	CreateCategory(string) error
	DelCategory(string) error
	AddRequest(string) error
	AddAllocation(string) error
}
