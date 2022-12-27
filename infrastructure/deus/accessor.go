package deus

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/url"
	"os"

	infra "github.com/Apiara/ApiaraCDN/infrastructure"
)

/*
internalDataAccessor represents an object that can retrieve data
from the internal network data stores.
*/
type internalDataAccessor interface {
	GetMetadata(cid string, mdata interface{}) error
	GetKey(cid string) ([]byte, error)
	GetContent(internalURL string, key []byte, out io.Writer) error
}

/*
aesInternalDataAccessor implements internalDataAccessor in a way that
assumes the 'key' parameter to GetContent is an AES key and that metadata
'mdata' parameter in GetMetadata is a JSON structure
*/
type aesInternalDataAccessor struct {
	metadataBaseURL string
	keyBaseURL      string
	contentBaseURL  string
	retrieveFile    func(string, io.Writer) error
}

/*
GetMetadata retrieves the content map associates with 'cid' and unmarshals it
to 'mdata' using the json.Unmarshal function
*/
func (d *aesInternalDataAccessor) GetMetadata(cid string, mdata interface{}) error {
	safeCid := infra.URLToSafeName(cid)
	mapURL, err := url.JoinPath(d.metadataBaseURL, safeCid)
	if err != nil {
		return fmt.Errorf("Failed to create %s metadata download path: %w", cid, err)
	}

	var mapBuf bytes.Buffer
	if err = d.retrieveFile(mapURL, &mapBuf); err != nil {
		return fmt.Errorf("Failed to download %s metadata: %w", cid, err)
	}

	if json.Unmarshal(mapBuf.Bytes(), mdata); err != nil {
		return fmt.Errorf("Failed to unmarshal %s metadata: %w", cid, err)
	}
	return nil
}

// GetKey returns the AES key associated with 'cid'
func (d *aesInternalDataAccessor) GetKey(cid string) ([]byte, error) {
	safeCid := infra.URLToSafeName(cid)
	keyURL, err := url.JoinPath(d.keyBaseURL, safeCid)
	if err != nil {
		return nil, fmt.Errorf("Failed to create %s key URL: %w", cid, err)
	}

	var cryptKey bytes.Buffer
	if err = d.retrieveFile(keyURL, &cryptKey); err != nil {
		return nil, fmt.Errorf("Failed to download %s key: %w", cid, err)
	}
	return cryptKey.Bytes(), nil
}

// GetContent writes the internal content at 'url' decrypted with 'key' to 'out'
func (d *aesInternalDataAccessor) GetContent(url string, key []byte, out io.Writer) error {
	// Download content
	file, err := ioutil.TempFile(os.TempDir(), "")
	if err != nil {
		return fmt.Errorf("Failed to open temp file for %s download: %w", url, err)
	}
	defer os.Remove(file.Name())

	if err = d.retrieveFile(url, file); err != nil {
		return fmt.Errorf("Failed to download file %s: %w", url, err)
	}
	file.Close()

	// Setup cipher block
	file, err = os.Open(file.Name())
	if err != nil {
		return fmt.Errorf("Failed to open downloaded %s file: %w", url, err)
	}
	defer file.Close()

	cipherBlock, err := aes.NewCipher(key)
	if err != nil {
		return fmt.Errorf("Failed to create block cipher from %s key: %w", url, err)
	}

	// Retrieve initialization vector
	iv := make([]byte, aes.BlockSize)
	_, err = file.Read(iv)
	if err != nil {
		return fmt.Errorf("Failed to extract IV from %s: %w", url, err)
	}

	// Decrypt data
	streamCipher := cipher.NewCTR(cipherBlock, iv)
	cryptWriter := &cipher.StreamWriter{S: streamCipher, W: out}
	if _, err = io.Copy(cryptWriter, file); err != nil {
		return fmt.Errorf("Failed to decrypt %s: %w", url, err)
	}
	return nil
}
