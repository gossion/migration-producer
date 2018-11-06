// TODO: license
// copied from https://github.com/pivotal-cf/goblob/blob/7688999e5678dda7d0d1c662f9ae9e8cf9adfd3f/blobstore/nfs_bucket_iterator.go

package blobstore

import (
	"errors"

	"github.com/gossion/migration-producer/pkg/datatype"
)

var ErrIteratorDone = errors.New("no more items in iterator")
var ErrIteratorAborted = errors.New("iterator aborted")

type nfsBucketIterator struct {
	blobCh chan *datatype.Blob
	doneCh chan struct{}
	errCh  chan error
}

func (i *nfsBucketIterator) Next() (*datatype.Blob, error) {
	if i.blobCh == nil {
		return nil, ErrIteratorDone
	}

	select {
	case blob, ok := <-i.blobCh:
		if !ok {
			i.blobCh = nil
			return nil, ErrIteratorDone
		}

		return blob, nil
	case err := <-i.errCh:
		if err != nil {
			return nil, err
		}
		return nil, ErrIteratorDone
	}
}

func (i *nfsBucketIterator) Done() {
	i.blobCh = nil
	close(i.doneCh)
}
