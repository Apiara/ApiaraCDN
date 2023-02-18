package damocles

import "fmt"

/*
StateMetadata represents an object that can read what content
a server is expected to be serving
*/
type StateMetadata interface {
	ServerContentList(serverID string) ([]string, error)
}

// LoadCategories makes updater consistent with what content is expected to be served
func LoadCategories(regionID string, metadata StateMetadata, updater CategoryUpdater) error {
	errMsg := "failed to load content serving state for server(%s): %w"

	// Retrieve expected content
	content, err := metadata.ServerContentList(regionID)
	if err != nil {
		return fmt.Errorf(errMsg, regionID, err)
	}

	// Update categories being served
	for _, cid := range content {
		if err = updater.CreateCategory(cid); err != nil {
			return fmt.Errorf(errMsg, regionID, err)
		}
	}
	return nil
}
