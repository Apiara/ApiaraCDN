package cyprus

import (
	"fmt"
	"log"
	"net/http"
	"path"

	infra "github.com/Apiara/ApiaraCDN/infrastructure"
)

type nonListableFileSystem struct {
	fs http.FileSystem
}

func (n *nonListableFileSystem) Open(path string) (http.File, error) {
	file, err := n.fs.Open(path)
	if err != nil {
		return nil, err
	}

	stat, err := file.Stat()
	if err != nil {
		return nil, err
	}

	if stat.IsDir() {
		return nil, fmt.Errorf("directory listing not allowed")
	}
	return file, nil
}

// StartStorageAPI starts the API used for accessing the processed data
func StartStorageAPI(listenAddr, storageDir string) {
	// Create file directory paths
	keyDir := path.Join(storageDir, infra.AESKeyStorageDir)
	dataDir := path.Join(storageDir, infra.CryptDataStorageDir)
	partialMapDir := path.Join(storageDir, infra.PartialMapDir)
	completeMapDir := path.Join(storageDir, infra.CompleteMediaMapDir)

	// Create file servers
	keyServer := http.FileServer(&nonListableFileSystem{http.Dir(keyDir)})
	cryptDataServer := http.FileServer(&nonListableFileSystem{http.Dir(dataDir)})
	partialMapServer := http.FileServer(&nonListableFileSystem{http.Dir(partialMapDir)})
	completeMapServer := http.FileServer(&nonListableFileSystem{http.Dir(completeMapDir)})

	// Create and start API
	storageAPI := http.NewServeMux()
	storageAPI.Handle(
		infra.CyprusStorageAPIKeyResource,
		http.StripPrefix(infra.CyprusStorageAPIKeyResource, keyServer),
	)
	storageAPI.Handle(
		infra.CyprusStorageAPIDataResource,
		http.StripPrefix(infra.CyprusStorageAPIDataResource, cryptDataServer),
	)
	storageAPI.Handle(
		infra.CyprusStorageAPIPartialMetadataResource,
		http.StripPrefix(infra.CyprusStorageAPIPartialMetadataResource, partialMapServer),
	)
	storageAPI.Handle(
		infra.CyprusStorageAPICompleteMetadataResource,
		http.StripPrefix(infra.CyprusStorageAPICompleteMetadataResource, completeMapServer),
	)
	log.Fatal(http.ListenAndServe(listenAddr, storageAPI))
}
