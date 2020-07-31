package puller

import (
	`golang.org/x/exp/mmap`
)

type MmapReader struct {
	off    int64
	end    int64
	reader *mmap.ReaderAt
}

func NewMmapReader(fileName string) (*MmapReader, error) {
	reader, err := mmap.Open(fileName)
	if err != nil {
		return nil, err
	}
	return &MmapReader{
		reader: reader,
		end: int64(reader.Len()),
	}, nil
}

func (reader *MmapReader) Read(p []byte) (n int, err error) {
	reader.off += int64(len(p))
	if reader.off > reader.end {
		reader.off = reader.end
	}
	n, err = reader.reader.ReadAt(p, reader.off)
	return n, err
}
