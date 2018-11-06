package migrator

import (
	"fmt"

	"github.com/gossion/migration-producer/pkg/blobstore"
	"github.com/gossion/migration-producer/pkg/datatype"
)

type BlobstoreMigrator interface {
	Migrate() error
}

type blobstoreMigrator struct {
	dst datatype.Blobstore
	src datatype.Blobstore
}

func NewblobstoreMigrator(dst datatype.Blobstore, src datatype.Blobstore) BlobstoreMigrator {
	return &blobstoreMigrator{
		dst: dst,
		src: src,
	}
}

func (m *blobstoreMigrator) Migrate() error {
	//TODO: check drv compatible
	drv, err := blobstore.GetDriver(m.src.Protocal)

	blobs, err := drv.List(m.src)
	if err != nil {
		return err
	}

	//TODO: seperate blobmigrator
	for _, blob := range blobs {
		reader, err := drv.Read(m.src, blob)
		if err != nil {
			return fmt.Errorf("error reading blob at %s: %s", blob.Path, err)
		}
		defer reader.Close()

		err = drv.Write(m.dst, blob, reader)
		if err != nil {
			return fmt.Errorf("error writing blob at %s: %s", blob.Path, err)
		}

		checksum, err := drv.Checksum(m.dst, blob)
		if err != nil {
			return fmt.Errorf("error checksumming blob at %s: %s", blob.Path, err)
		}

		if checksum != blob.Checksum {
			return fmt.Errorf(
				"error at %s: checksum [%s] does not match [%s]",
				blob.Path,
				checksum,
				blob.Checksum,
			)
		}

	}

	return nil
}
