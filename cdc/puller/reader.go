package puller

type MmapReader struct {
	off    int64
	reader *ReaderAt
}

func NewMmapReader(fileName string) (*MmapReader, error) {
	reader, err := Open(fileName)
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
