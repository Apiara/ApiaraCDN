package cyprus

import (
  "os"
  "io"
  "testing"
  "io/ioutil"
  "crypto/aes"
  "crypto/cipher"
  "github.com/stretchr/testify/assert"
)

func createTestIngestFile(fname string) (string, error) {
  outFile, err := ioutil.TempFile("./test_resources/working/", ingestFilePattern)
  if err != nil {
    return "", err
  }
  defer outFile.Close()

  inFile, err := os.Open(fname)
  if err != nil {
    return "", err
  }
  defer inFile.Close()

  _, err = io.Copy(outFile, inFile)
  if err != nil {
    return "", err
  }
  return outFile.Name(), nil
}

func TestAESProcessorWithRawMedia(t *testing.T) {
  // Create test resources
  rawFname := "./test_resources/hls/index_1_1.ts"
  ingestName, err := createTestIngestFile(rawFname)
  if err != nil {
    t.Fatalf("Failed to create test ingest file: %v", err)
  }

  // Cleanup
  defer func() {
    os.Remove(ingestName)
    os.RemoveAll("./test_resources/working/")
    os.MkdirAll("./test_resources/working/", 0777)
  }()

  rawIngest := MediaIngest{
    Type: RawMedia,
    Result: rawMedia{
      URL: rawFname,
      FunctionalID: "",
      Checksum: "",
      File: ingestName,
    },
  }

  // Digest
  processor, err := NewAESDataProcessor(DefaultAESKeySize, "./test_resources/working")
  if err != nil {
    t.Fatalf("Failed to create aes data processor: %v", err)
  }

  digest, err := processor.DigestMedia(rawIngest)
  if err != nil {
    t.Fatalf("Failed to digest raw media: %v", err)
  }

  // Check output validity
  assert.Equal(t, digest.Type, RawMedia, "Failed to tag digest correctly")
  assert.Equal(t, len(digest.CryptKey), DefaultAESKeySize, "Failed to create aes key")
  if digest.FunctionalID == "" || digest.FunctionalID == rawFname {
    t.Fatalf("Failed to create obfuscated functional ID. Got %s", digest.FunctionalID)
  }

  // Decrypt and check content integrity
  media := digest.Result.(rawMedia)
  cipherData, err := ioutil.ReadFile(media.File)
  if err != nil {
    t.Fatalf("Failed to read digest file: %v", err)
  }

  iv := cipherData[:aes.BlockSize]
  ciphertext := cipherData[aes.BlockSize:]
  block, err := aes.NewCipher(digest.CryptKey)
  if err != nil {
    t.Fatalf("Failed to use crypt key in digest: %v", err)
  }

  plaintext := make([]byte, len(ciphertext))
  cipherStream := cipher.NewCTR(block, iv)
  cipherStream.XORKeyStream(plaintext, ciphertext)

  assert.Equal(t, string(plaintext), "test data\n", "Failed to properly decrypt data. Got " + string(plaintext))
}

func TestAESProcessorWithManifest(t *testing.T) {
  // Create test resources
  hlsFname := "./test_resources/hls/master.m3u8"
  ingestFileName, err := createTestIngestFile("./test_resources/hls/index_1_1.ts")
  if err != nil {
    t.Fatalf("Failed to create test ingest file: %v", err)
  }

  // Cleanup
  defer func() {
    os.Remove(ingestFileName)
    os.RemoveAll("./test_resources/working/")
    os.MkdirAll("./test_resources/working/", 0777)
  }()

  // Create test MediaIngest
  hlsIngest := MediaIngest{
    Type: VODMedia,
    Result: manifest{
      URL: hlsFname,
      FunctionalID: "",
      Streams: []stream{
        stream{
          URL: "./test_resources/hls/index_1.m3u8",
          FunctionalID: "",
          Segments: []segment{
            segment{
              Index: 1,
              URL: "./test_resourcecs/hls/index_1_1.ts",
              FunctionalID: "",
              Checksum: "",
              File: ingestFileName,
            },
          },
        },
      },
    },
  }

  // Digest
  processor, err := NewAESDataProcessor(DefaultAESKeySize, "./test_resources/working")
  if err != nil {
    t.Fatalf("Failed to create aes data processor: %v", err)
  }

  digest, err := processor.DigestMedia(hlsIngest)
  if err != nil {
    t.Fatalf("Failed to digest hls ingest: %v", err)
  }

  // Check output validity
  assert.Equal(t, digest.Type, VODMedia, "Failed to tag digest correctly")
  assert.Equal(t, len(digest.CryptKey), DefaultAESKeySize, "Failed to create aes key")
  if digest.FunctionalID == "" || digest.FunctionalID == hlsFname {
    t.Fatalf("Failed to create obfuscated functional ID. Got %s", digest.FunctionalID)
  }

  mediaManifest, ok := digest.Result.(manifest)
  if !ok {
    t.Fatalf("Failed to return the correct digest type for manifest")
  }
  if len(mediaManifest.Streams[0].Segments[0].Checksum) == 0 {
    t.Fatalf("Failed to create data checksum")
  }
}
