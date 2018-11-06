package datatype

type Blobstore struct {
	Path     string
	Protocal string
}

type Blob struct {
	Checksum string
	Path     string
}

type BlobIterator interface {
	Next() (*Blob, error)
	Done()
}
