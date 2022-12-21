package deus

type DataValidator interface {
	IsStale(cid string) (bool, error)
}

type SimpleDataValidator struct {
}

func (s *SimpleDataValidator) IsStale(cid string) (bool, error) {
	// Download sample from cid
	// Find equivalent internal sample
	// compare samples, if equal return false, else true
}
