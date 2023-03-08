package infrastructure

import (
	"bytes"
	"crypto/sha256"
	"encoding/gob"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"net/url"
)

/*
URLToSafeName converts URL with possible unsafe
characters to a unique hex string 24 bytes long
*/
func URLToSafeName(url string) string {
	sum := sha256.Sum224([]byte(url))
	safe := hex.EncodeToString(sum[:])
	return safe
}

// RequestBodyDecoder represents a function that can decode a HTTP response body
type RequestBodyDecoder func(io.Reader, interface{}) error

func StringBodyDecoder(in io.Reader, result interface{}) error {
	var buf bytes.Buffer
	if _, err := io.Copy(&buf, in); err != nil {
		return err
	}

	strResult := result.(*string)
	*strResult = buf.String()
	return nil
}

// GOBBodyDecoder decodes in using GOB. Note that 'result' must be a GOB registered object
func GOBBodyDecoder(in io.Reader, result interface{}) error {
	if err := gob.NewDecoder(in).Decode(result); err != nil {
		return err
	}
	return nil
}

// MakeHTTPRequest is a generic function for making an HTTP request and receiving/decoding a body response
func MakeHTTPRequest(url string, query url.Values, body io.Reader,
	client *http.Client, dec RequestBodyDecoder, result interface{}) error {
	// Create HTTP request
	req, err := http.NewRequest(http.MethodGet, url, body)
	if err != nil {
		return err
	}
	req.URL.RawQuery = query.Encode()

	// Perform request and check for failures
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("bad HTTP status: %s", resp.Status)
	}

	// Unmarshal response body into result using gob
	if result != nil {
		if err = dec(resp.Body, result); err != nil {
			return err
		}
	}
	return nil
}
