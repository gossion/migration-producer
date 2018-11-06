// TODO: changed from https://raw.githubusercontent.com/pivotal-cf/godatatype.Blob/7688999e5678dda7d0d1c662f9ae9e8cf9adfd3f/datatype.Blobstore/nfs.go
// check license

package blobstore

import (
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"

	"github.com/cheggaaa/pb"
	"github.com/gossion/migration-producer/pkg/datatype"
	"github.com/gossion/migration-producer/pkg/utils"
	"golang.org/x/sync/errgroup"
)

func init() {
	RegisterDriver(NfsDriver{}, "nfs")
}

type NfsDriver struct {
}

// List fetches a list of files with checksums
func (drv NfsDriver) List(s datatype.Blobstore) ([]*datatype.Blob, error) {
	var blobs []*datatype.Blob
	walk := func(path string, info os.FileInfo, e error) error {
		if !info.IsDir() && info.Name() != ".nfs_test" {
			relPath := path[len(s.Path)+1:]
			blobs = append(blobs, &datatype.Blob{
				Path: relPath,
			})
		}
		return e
	}
	if err := filepath.Walk(s.Path, walk); err != nil {
		return nil, err
	}
	if err := drv.processBlobsForChecksums(s, blobs); err != nil {
		return nil, err
	}
	return blobs, nil
}

func (drv NfsDriver) processBlobsForChecksums(s datatype.Blobstore, blobs []*datatype.Blob) error {

	fmt.Println("Getting list of files from NFS")
	bar := pb.StartNew(len(blobs))
	bar.Format("<.- >")

	var g errgroup.Group
	for _, blob := range blobs {
		blob := blob
		g.Go(func() error {
			checksum, err := drv.Checksum(s, blob)
			if (err) != nil {
				return err
			}
			blob.Checksum = checksum
			bar.Increment()
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return err
	}

	bar.FinishPrint("Done Getting list of files from NFS")
	return nil
}

func (drv NfsDriver) Checksum(s datatype.Blobstore, b *datatype.Blob) (string, error) {
	return utils.Checksum(path.Join(s.Path, b.Path))
}

func (drv NfsDriver) Read(s datatype.Blobstore, b *datatype.Blob) (io.ReadCloser, error) {
	return os.Open(path.Join(s.Path, b.Path))
}

//TODO: src io.ReadCloser? origin io.Reader
func (drv NfsDriver) Write(s datatype.Blobstore, b *datatype.Blob, src io.ReadCloser) error {
	f, err := os.Open(path.Join(s.Path, b.Path))
	if err != nil {
		return err
	}
	defer f.Close()

	_, err = io.Copy(f, src)
	if err != nil {
		return err
	}

	return nil
}

func (drv NfsDriver) Exists(s datatype.Blobstore, b *datatype.Blob) bool {
	checksum, err := drv.Checksum(s, b)
	if err != nil {
		return false
	}

	return checksum == b.Checksum
}

func (drv NfsDriver) NewBlobIterator(folder string) (datatype.BlobIterator, error) {
	return nil, nil
}

// TODO: check
/*
func (drv NfsDriver) NewBucketIterator(folder string) (datatype.BlobIterator, error) {
	blobCh := make(chan *datatype.Blob)
	doneCh := make(chan struct{})
	errCh := make(chan error)

	actualPath := filepath.Join(s.path, folder)

	files, err := ioutil.ReadDir(actualPath)
	if err != nil {
		return nil, err
	}

	if len(files) == 0 {
		return &nfsBucketIterator{}, nil
	}

	iterator := &nfsBucketIterator{
		blobCh: blobCh,
		doneCh: doneCh,
		errCh:  errCh,
	}

	walkFn := func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if info.IsDir() || info.Name() == ".nfs_test" {
			return nil
		}

		select {
		case <-doneCh:
			doneCh = nil
			return ErrIteratorAborted
		default:
			blob := &datatype.Blob{
				Path: strings.TrimPrefix(path, s.path+string(os.PathSeparator)),
			}

			blobCh <- blob

			return nil
		}
	}

	go func() {
		errCh <- filepath.Walk(actualPath, walkFn)
		close(blobCh)
	}()

	return iterator, nil
}
*/
