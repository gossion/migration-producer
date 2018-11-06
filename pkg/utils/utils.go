package utils

import (
	"bytes"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"io"
	"log"
	"os"
	"os/exec"
	"strings"
)

// RunCommand runs a command and returns the stdout if successful
func RunCommand(name string, args ...string) ([]byte, error) {
	log.Println("exec", name, args)
	var stdout, stderr bytes.Buffer
	cmd := exec.Command(name, args...)
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		// return stderr if available
		if s := strings.TrimSpace(stderr.String()); s != "" {
			return nil, errors.New(s)
		}

		// otherwise return error
		return nil, err
	}

	// return stdout
	return stdout.Bytes(), nil
}

// TODO: return stderr ?
// 		 log stderr and only return error ?
// TODO: o *bufio.Writer -> io.Writer
func RunCommandOutTOFile(name string, o io.Writer, args ...string) ([]byte, error) {
	log.Println("exec", name, args)

	var stderr bytes.Buffer
	cmd := exec.Command(name, args...)
	cmd.Stdout = o
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		// return stderr if available
		if s := strings.TrimSpace(stderr.String()); s != "" {
			return nil, errors.New(s)
		}

		// otherwise return error
		return nil, err
	}

	// return stdout
	return stderr.Bytes(), nil
}

// RunCommand runs a command and returns the stdout if successful
func RunCommandWithStdin(name string, i io.Reader, args ...string) ([]byte, error) {
	log.Println("exec", name, args)
	var stdout, stderr bytes.Buffer
	cmd := exec.Command(name, args...)
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	cmd.Stdin = i

	if err := cmd.Run(); err != nil {
		// return stderr if available
		if s := strings.TrimSpace(stderr.String()); s != "" {
			return nil, errors.New(s)
		}

		// otherwise return error
		return nil, err
	}

	// return stdout
	return stdout.Bytes(), nil
}

// Checksum calculates a checksum for a file
func Checksum(filePath string) (string, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return "", err
	}
	defer file.Close()

	return ChecksumReader(file)
}

// ChecksumReader calculates a checksum for a file
func ChecksumReader(reader io.Reader) (string, error) {
	hash := md5.New()
	if _, err := io.Copy(hash, reader); err != nil {
		return "", err
	}

	hashInBytes := hash.Sum(nil)[:16]
	return hex.EncodeToString(hashInBytes), nil
}
