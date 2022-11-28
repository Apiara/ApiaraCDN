package cyprus

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"io"
	"io/ioutil"
	"os"
)

var (
	// Default to strongest encryption(AES-256)
	DefaultAESKeySize = 32
)

const (
	digestFilePatter = "digest_*"
)

// MediaDigest is the result type of a DataProcessor
type MediaDigest struct {
	Type         MediaType
	CryptKey     []byte
	FunctionalID string
	ByteSize     int64
	Result       interface{}
}

/*
DataProcessor represents an object that processes a MediaIngest returned
from a DataPreprocessor and handles encryption + complete content map creation
*/
type DataProcessor interface {
	DigestMedia(ingest MediaIngest) (MediaDigest, error)
}

/*
AESDataProcessor implements DataProcessor using AES for media encryption
and a combination of SHA256 + AES for creation of Functional media IDs
*/
type AESDataProcessor struct {
	keySize   int
	outputDir string
}

// NewAESDataProcessor creates a new AESDataProcessor with the specified key size
func NewAESDataProcessor(keySize int, workingDir string) (*AESDataProcessor, error) {
	if keySize != 16 && keySize != 24 && keySize != 32 {
		return nil, fmt.Errorf("Failed to create AESDataProcessor. Invalid Key Size %d", keySize)
	}
	return &AESDataProcessor{
		keySize:   keySize,
		outputDir: workingDir,
	}, nil
}

func generateRandomBytes(size int) ([]byte, error) {
	key := make([]byte, size)
	_, err := rand.Read(key)
	if err != nil {
		return nil, err
	}
	return key, nil
}

func generateFunctionalID(id string, streamCipher cipher.Stream) string {
	checksum := sha256.Sum256([]byte(id))
	functionalID := checksum[:]
	streamCipher.XORKeyStream(functionalID, functionalID)
	return hex.EncodeToString(functionalID)
}

func calculateSHA256Checksum(fname string) ([]byte, error) {
	// Open file to calclulate checksum for
	file, err := os.Open(fname)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	// Calculate SHA256 Checksum of encrypted data
	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		return nil, err
	}
	return hash.Sum(nil), nil
}

/*
digestFile creates a file with the first a.keySize bytes being the initialization
vector and the remaining data being 'fname' files content encrypted in CTR mode. Returns
output file name, file checksum, file size, and error
*/
func (a *AESDataProcessor) digestFile(block cipher.Block, fname string) (string, string, int64, error) {
	/* Ensure digest always deletes ingest file. Prevents buildup
	of data on disk due to failed digests */
	defer os.Remove(fname)

	// Generate initialization vector for media encryption
	iv, err := generateRandomBytes(aes.BlockSize)
	if err != nil {
		return "", "", -1, err
	}

	// Create encrypted file, prepend initialization vector
	outFile, err := ioutil.TempFile(a.outputDir, digestFilePatter)
	if err != nil {
		return "", "", -1, err
	}

	_, err = outFile.Write(iv)
	if err != nil {
		outFile.Close()
		return "", "", -1, fmt.Errorf("Failed to prepend initialization vector to digest: %w", err)
	}

	// Encrypt segment using block+iv in CTR mode and write digest
	streamCipher := cipher.NewCTR(block, iv)
	cryptWriter := &cipher.StreamWriter{S: streamCipher, W: outFile}

	plainFile, err := os.Open(fname)
	if err != nil {
		outFile.Close()
		return "", "", -1, fmt.Errorf("Failed to open ingest file %s: %w", fname, err)
	}

	if _, err = io.Copy(cryptWriter, plainFile); err != nil {
		outFile.Close()
		plainFile.Close()
		return "", "", -1, fmt.Errorf("Failed to write encrypted data: %w", err)
	}
	outFile.Close()
	plainFile.Close()

	// Calculate checksum
	checksum, err := calculateSHA256Checksum(outFile.Name())
	if err != nil {
		return "", "", -1, fmt.Errorf("Failed to calculate checksum for file %s: %w", outFile.Name(), err)
	}

	// Get file size
	info, err := os.Stat(outFile.Name())
	if err != nil {
		return "", "", -1, fmt.Errorf("Failed to get size of file %s: %w", outFile.Name(), err)
	}

	return outFile.Name(), base64.StdEncoding.EncodeToString(checksum), info.Size(), nil
}

/*
digestRawMedia delegates to digestFile and returns a rawMedia
instance and the processed media size
*/
func (a *AESDataProcessor) digestRawMedia(block cipher.Block, media rawMedia) (rawMedia, int64, error) {
	// Create stream cipher for use in creating Functional ID
	fidIV, err := generateRandomBytes(aes.BlockSize)
	if err != nil {
		return rawMedia{}, -1, err
	}
	fidCipher := cipher.NewCTR(block, fidIV)

	// Update rawMedia entry
	var size int64
	media.FunctionalID = generateFunctionalID(media.URL, fidCipher)
	media.File, media.Checksum, size, err = a.digestFile(block, media.File)
	return media, size, err
}

/*
digestManifest takes a manifest and generates Functional IDs for each member of
the manifest. In addition to this, it encrypts all segment files in the passed in
manifest and returns a manifest with the File pointers pointing to the encrypted
data
*/
func (a *AESDataProcessor) digestManifest(block cipher.Block, mediaMap manifest) (manifest, int64, error) {
	// Create stream cipher used to assist in creation of Functional IDs
	fidIV, err := generateRandomBytes(aes.BlockSize)
	if err != nil {
		return manifest{}, -1, err
	}
	fidCipher := cipher.NewCTR(block, fidIV)

	// Modify manifest with generated functional IDs and new encrypted segment locations
	fileSize := int64(0)
	totalSize := int64(0)
	mediaMap.FunctionalID = generateFunctionalID(mediaMap.URL, fidCipher)
	completeStreams := make([]stream, 0)
	for _, mediaStream := range mediaMap.Streams {
		mediaStream.FunctionalID = generateFunctionalID(mediaStream.URL, fidCipher)
		completeSegments := make([]segment, 0)
		for _, mediaSegment := range mediaStream.Segments {
			mediaSegment.FunctionalID = generateFunctionalID(mediaSegment.URL, fidCipher)
			mediaSegment.File, mediaSegment.Checksum, fileSize, err = a.digestFile(block, mediaSegment.File)
			if err != nil {
				return manifest{}, -1, err
			}
			totalSize += fileSize
			completeSegments = append(completeSegments, mediaSegment)
		}
		mediaStream.Segments = completeSegments
		completeStreams = append(completeStreams, mediaStream)
	}
	mediaMap.Streams = completeStreams
	return mediaMap, totalSize, nil
}

// DigestMedia takes a MediaIngest and encrypts the data using AES, returning a MediaDigest
func (a *AESDataProcessor) DigestMedia(ingest MediaIngest) (MediaDigest, error) {
	// Randomly generate cryptographically secure 256-bit key and initialization vector
	aesKey, err := generateRandomBytes(a.keySize)
	if err != nil {
		return MediaDigest{}, fmt.Errorf("Failed to generate symmetric key of size %d: %w", a.keySize, err)
	}

	// Create stream cipher
	block, err := aes.NewCipher(aesKey)
	if err != nil {
		return MediaDigest{}, fmt.Errorf("Failed to generate cipher block: %w", err)
	}

	// Delegate processing and create digest
	digest := MediaDigest{CryptKey: aesKey, Type: ingest.Type}
	switch ingest.Type {
	case RawMedia:
		media, size, err := a.digestRawMedia(block, ingest.Result.(rawMedia))
		if err != nil {
			return MediaDigest{}, fmt.Errorf("Failed to digest raw media file: %w", err)
		}
		digest.Result = media
		digest.FunctionalID = media.FunctionalID
		digest.ByteSize = size
		break
	case VODMedia:
		mediaMap, size, err := a.digestManifest(block, ingest.Result.(manifest))
		if err != nil {
			return MediaDigest{}, fmt.Errorf("Failed to digest manifest: %w", err)
		}
		digest.Result = mediaMap
		digest.FunctionalID = mediaMap.FunctionalID
		digest.ByteSize = size
		break
	default:
		return MediaDigest{}, fmt.Errorf("Invalid ingest type: %w", err)
	}

	return digest, nil
}
