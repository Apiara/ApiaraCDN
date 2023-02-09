package crow

import (
	"fmt"

	"github.com/Apiara/ApiaraCDN/infrastructure/state"
)

/*
StateMetadata represents an object that can access all network
state information that may be needed by crow
*/
type StateMetadata interface {
	state.ContentMetadataStateReader
	ServerList() ([]string, error)
	ServerContentList(serverID string) ([]string, error)
}

// LoadContent makes allocator consistent with what content is expected to be allocated on the network
func LoadContent(metadata StateMetadata, allocator LocationAwareDataAllocator) error {
	// Get list of all servers
	errMsg := "failed to load content allocation state: %w"
	servers, err := metadata.ServerList()
	if err != nil {
		return fmt.Errorf(errMsg, err)
	}

	// Update allocator based on what content is served where
	contentSize := make(map[string]int64)
	for _, server := range servers {
		serving, err := metadata.ServerContentList(server)
		if err != nil {
			return fmt.Errorf(errMsg, err)
		}

		for _, cid := range serving {
			if _, ok := contentSize[cid]; !ok {
				contentSize[cid], err = metadata.GetContentSize(cid)
				if err != nil {
					return fmt.Errorf(errMsg, err)
				}
			}
			err = allocator.NewEntry(server, cid, contentSize[cid])
			if err != nil {
				return fmt.Errorf(errMsg, err)
			}
		}
	}
	return nil
}
