package cypress

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/md5"
	"crypto/sha1"
	"crypto/sha256"
	"errors"
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
	if key == nil || len(key) == 0 {
		return nil, errors.New("key cannot be null or empty")
	}

	if iv == nil || len(iv) == 0 {
		return nil, errors.New("iv cannot be null or empty")
	}

	if data == nil || len(data) == 0 {
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
	if key == nil || len(key) == 0 {
		return nil, errors.New("key cannot be null or empty")
	}

	if iv == nil || len(iv) == 0 {
		return nil, errors.New("iv cannot be null or empty")
	}

	if data == nil || len(data) == 0 {
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
	if padding < 0 || int(padding) >= len(data) {
		return nil, errors.New("bad padding")
	}

	return data[:len(data)-int(padding)], nil
}
