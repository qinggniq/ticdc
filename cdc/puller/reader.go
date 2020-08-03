package puller

import (
	`golang.org/x/exp/mmap`
)

type MmapReader struct {
	off    int64
	reader *mmap.ReaderAt
}

func NewMmapReader(fileName string) (*MmapReader, error) {
	reader, err := mmap.Open(fileName)
	if err != nil {
		return nil, err
	}
	return &MmapReader{
		reader: reader,
	}, nil
}

func (reader *MmapReader) Read(p []byte) (n int, err error) {
	n, err = reader.reader.ReadAt(p, reader.off)
	reader.off += int64(len(p))
	return n, err
}
