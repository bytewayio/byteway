package cypress

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/md5"
	"crypto/rsa"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"os"
	"strings"
)

const (
	pemTypeRSAPrivateKey = "RSA PRIVATE KEY"
	pemTypePublicKey     = "PUBLIC KEY"
)

// Md5 returns the md5 checksum of the data
func Md5(data []byte) []byte {
	sum := md5.Sum(data)
	return sum[:]
}

// Sha256 returns the sha256 checksum of the data
func Sha256(data []byte) []byte {
	sum := sha256.Sum256(data)
	return sum[:]
}

// Sha1 returns the sha1 checksum of the data
func Sha1(data []byte) []byte {
	sum := sha1.Sum(data)
	return sum[:]
}

// Aes256Encrypt encrypts the data with given key and iv using AES256/CBC/PKCS5Padding
func Aes256Encrypt(key, iv, data []byte) ([]byte, error) {
	if len(key) == 0 {
		return nil, errors.New("key cannot be null or empty")
	}

	if len(iv) == 0 {
		return nil, errors.New("iv cannot be null or empty")
	}

	if len(data) == 0 {
		return nil, errors.New("data cannot be null or empty")
	}

	keyHash := Sha256(key)
	ivHash := Sha256(iv)
	block, err := aes.NewCipher(keyHash)
	if err != nil {
		return nil, err
	}

	ecb := cipher.NewCBCEncrypter(block, ivHash[0:aes.BlockSize])
	content := pkcs5Padding(data, block.BlockSize())
	encrypted := make([]byte, len(content))
	ecb.CryptBlocks(encrypted, content)
	return encrypted, nil
}

// Aes256Decrypt decrypts the data with given key and iv using AES256/CBC/PKCS5Padding
func Aes256Decrypt(key, iv, data []byte) ([]byte, error) {
	if len(key) == 0 {
		return nil, errors.New("key cannot be null or empty")
	}

	if len(iv) == 0 {
		return nil, errors.New("iv cannot be null or empty")
	}

	if len(data) == 0 {
		return nil, errors.New("data cannot be null or empty")
	}

	keyHash := Sha256(key)
	ivHash := Sha256(iv)
	block, err := aes.NewCipher(keyHash)
	if err != nil {
		return nil, err
	}

	ecb := cipher.NewCBCDecrypter(block, ivHash[0:aes.BlockSize])
	decrypted := make([]byte, len(data))
	ecb.CryptBlocks(decrypted, data)

	return pkcs5Trimming(decrypted)
}

func pkcs5Padding(ciphertext []byte, blockSize int) []byte {
	padding := blockSize - len(ciphertext)%blockSize
	padtext := bytes.Repeat([]byte{byte(padding)}, padding)
	return append(ciphertext, padtext...)
}

func pkcs5Trimming(data []byte) ([]byte, error) {
	padding := data[len(data)-1]
	if int(padding) >= len(data) {
		return nil, errors.New("bad padding")
	}

	return data[:len(data)-int(padding)], nil
}

// DecodeRsaPrivateKey decode rsa private key from pem payload
func DecodeRsaPrivateKey(payload []byte) (*rsa.PrivateKey, error) {
	block, _ := pem.Decode(payload)
	if block == nil || len(block.Bytes) == 0 {
		return nil, errors.New("invalid pem file")
	}

	if block.Type != pemTypeRSAPrivateKey {
		return nil, errors.New("invalid key type")
	}

	var parsedKey interface{}
	var err error
	if parsedKey, err = x509.ParsePKCS1PrivateKey(block.Bytes); err != nil {
		if parsedKey, err = x509.ParsePKCS8PrivateKey(block.Bytes); err != nil {
			return nil, errors.New("failed to parse RSA private key")
		}
	}

	if privateKey, ok := parsedKey.(*rsa.PrivateKey); ok {
		return privateKey, nil
	}

	return nil, errors.New("not an RSA private key")
}

// LoadRsaPrivateKey load rsa private key from pem file
func LoadRsaPrivateKey(pemFile string) (*rsa.PrivateKey, error) {
	fileContent, err := os.ReadFile(pemFile)
	if err != nil {
		return nil, err
	}

	return DecodeRsaPrivateKey(fileContent)
}

// DecodeRsaPublicKey decodes rsa public key from pem payload
func DecodeRsaPublicKey(payload []byte) (*rsa.PublicKey, error) {
	block, _ := pem.Decode(payload)
	if block == nil || len(block.Bytes) == 0 {
		return nil, errors.New("invalid pem file")
	}

	if !strings.HasSuffix(block.Type, pemTypePublicKey) {
		return nil, errors.New("invalid key type")
	}

	var parsedKey interface{}
	var err error
	if parsedKey, err = x509.ParsePKIXPublicKey(block.Bytes); err != nil {
		if parsedKey, err = x509.ParsePKCS1PublicKey(block.Bytes); err != nil {
			return nil, errors.New("failed to parse RSA public key")
		}
	}

	if publicKey, ok := parsedKey.(*rsa.PublicKey); ok {
		return publicKey, nil
	}

	return nil, errors.New("not an RSA public key")
}

// LoadRsaPublicKey load rsa public key from pem file
func LoadRsaPublicKey(pemFile string) (*rsa.PublicKey, error) {
	fileContent, err := os.ReadFile(pemFile)
	if err != nil {
		return nil, err
	}

	return DecodeRsaPublicKey(fileContent)
}
