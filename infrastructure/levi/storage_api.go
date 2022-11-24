package levi

import (
  "net/http"
  "fmt"
  "log"
)

type nonListableFileSystem struct {
  fs http.FileSystem
}

func (n nonListableFileSystem) Open(path string) (http.File, error) {
  file, err := n.fs.Open(path)
  if err != nil {
    return nil, err
  }

  stat, err := file.Stat()
  if err != nil {
    return nil, err
  }

  if stat.IsDir() {
    return nil, fmt.Errorf("Directory listing not allowed")
  }
  return file, nil
}

func StartStorageAPI(listenAddr, keyDir, dataDir, partialMapDir, completeMapDir string) {
  keyServer := http.FileServer(nonListableFileSystem{http.Dir(keyDir)})
  cryptDataServer := http.FileServer(nonListableFileSystem{http.Dir(dataDir)})
  partialMapServer := http.FileServer(nonListableFileSystem{http.Dir(partialMapDir)})
  completeMapServer := http.FileServer(nonListableFileSystem{http.Dir(completeMapDir)})

  storageAPI := http.NewServeMux()
  storageAPI.Handle("/url/key/", http.StripPrefix("/url/key", keyServer))
  storageAPI.Handle("/fid/crypt/", http.StripPrefix("/fid/crypt", cryptDataServer))
  storageAPI.Handle("/fid/pmap/", http.StripPrefix("/fid/pmap", partialMapServer))
  storageAPI.Handle("/url/cmap/", http.StripPrefix("/url/cmap", completeMapServer))
  log.Fatal(http.ListenAndServe(listenAddr, storageAPI))
}
