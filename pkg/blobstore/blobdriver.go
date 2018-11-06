package blobstore

import (
	"fmt"
	"io"

	"github.com/gossion/migration-producer/pkg/datatype"
)

type BlobstoreDriver interface {
	//Returns a list of all the "blobs" in blobstore
	List(s datatype.Blobstore) ([]*datatype.Blob, error)
	//For a given blob will return io.ReadCloser with contents
	Read(s datatype.Blobstore, b *datatype.Blob) (io.ReadCloser, error)
	//Returns md5 checksum for the given blob
	Checksum(s datatype.Blobstore, b *datatype.Blob) (string, error)
	//Writes the blob to the blobstore
	Write(s datatype.Blobstore, b *datatype.Blob, src io.ReadCloser) error
	//Determins if blob exists
	Exists(s datatype.Blobstore, b *datatype.Blob) bool
	//Returns an interator for all the blobs in the given store (folder for NFS)
	NewBlobIterator(folder string) (datatype.BlobIterator, error)

	//Mount
	//Umount
}

var drivers = map[string]BlobstoreDriver{}

//Register driver
func RegisterDriver(drv BlobstoreDriver, blobtype string) {
	drivers[blobtype] = drv
}

// GetDriver loads a blobstore driver by name
func GetDriver(name string) (BlobstoreDriver, error) {
	if val, ok := drivers[name]; ok {
		return val, nil
	}

	return nil, fmt.Errorf("unsupported driver: %s", name)
}
