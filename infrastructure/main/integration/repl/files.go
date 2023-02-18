package main

import (
	"net/http"
	"path/filepath"
	"strconv"
)

func serveDirectory(port int, dir string) {
	fullDir, err := filepath.Abs(dir)
	if err != nil {
		panic(err)
	}
	fs := http.FileServer(http.Dir(fullDir))
	http.ListenAndServe(":"+strconv.Itoa(port), fs)
}
