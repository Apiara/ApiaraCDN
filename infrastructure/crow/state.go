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
	type cinfo struct {
		fid  string
		size int64
	}
	contentInfo := make(map[string]*cinfo)
	for _, server := range servers {
		serving, err := metadata.ServerContentList(server)
		if err != nil {
			return fmt.Errorf(errMsg, err)
		}

		for _, cid := range serving {
			if _, ok := contentInfo[cid]; !ok {
				contentInfo[cid] = &cinfo{}
				contentInfo[cid].size, err = metadata.GetContentSize(cid)
				if err != nil {
					return fmt.Errorf(errMsg, err)
				}
				contentInfo[cid].fid, err = metadata.GetContentFunctionalID(cid)
				if err != nil {
					return fmt.Errorf(errMsg, err)
				}
			}
			err = allocator.NewEntry(server, contentInfo[cid].fid, contentInfo[cid].size)
			if err != nil {
				return fmt.Errorf(errMsg, err)
			}
		}
	}
	return nil
}
