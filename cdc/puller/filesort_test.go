package puller

import (
	`bufio`
	`bytes`
	`encoding/binary`
	`encoding/gob`
	`flag`
	`io`
	`log`
	`math/rand`
	`os`
	`path`
	`strconv`
	`strings`
	`testing`

	`github.com/pingcap/errors`
	`github.com/vmihailenco/msgpack/v5`

	`github.com/pingcap/ticdc/cdc/model`
)

var (
	EventNumber = *flag.Int("eventnumber", 10000, "eventnumber=<number of event> default=10000")
	FileNum     = *flag.Int("filenumber", 1, "filenumber=<number of file> default=1")
	DateDir     = *flag.String("datadir", "/tmp", "datadir=<file dir> default=/tmp")
	EventSize      = *flag.Int("eventsize", 1024, "eventsize=<size of every event> default=1024")
	defaultBufSize = 512
)

func TestMain(m *testing.M) {
	// call flag.Parse() here if TestMain uses flags

	defaultBufSize = EventSize / 2
	prepareData("msgpack")
	prepareData("gob")
	os.Exit(m.Run())
}

func genDataPath(eventNumber int, eventSize int, idx int, encodeType string) string {
	return path.Join(DateDir, strings.Join([]string{
		"file-sort",
		strconv.FormatInt(int64(eventNumber), 10),
		strconv.FormatInt(int64(eventSize), 10),
		strconv.FormatInt(int64(idx), 10),
		encodeType,
	}, "-"))
}

func prepareData(encodeType string) {
	data = generatePolymorphicEvents(EventSize)
	for i := 0; i < FileNum; i++ {
		fileName := genDataPath(EventNumber, EventSize, i, encodeType)
		if _, err := os.Stat(fileName); os.IsNotExist(err) {
			_, err := writeEventsToFile(fileName, data, encodeType)
			if err != nil {
				log.Printf("write data to file %s %v", fileName, err)
			}
		}
	}
}

func readPolymorphicEventSingle(rd io.Reader, readBuf *bytes.Reader) (*model.PolymorphicEvent, error) {
	var byteLen [8]byte
	n, err := io.ReadFull(rd, byteLen[:])
	if err != nil {
		if err == io.EOF {
			return nil, nil
		}
		return nil, errors.Trace(err)
	}
	if n < 8 {
		return nil, errors.Errorf("invalid length data %s, read %d bytes", byteLen, n)
	}
	dataLen := int(binary.BigEndian.Uint64(byteLen[:]))

	data := make([]byte, dataLen)
	n, err = io.ReadFull(rd, data)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if n != dataLen {
		return nil, errors.Errorf("truncated data %s n: %d dataLen: %d", data, n, dataLen)
	}

	//readBuf.Reset(data)
	//ev := &model.PolymorphicEvent{}
	//err = gob.NewDecoder(readBuf).Decode(ev)
	//if err != nil {
	//	return nil, errors.Trace(err)
	//}
	return &model.PolymorphicEvent{}, nil
}

func parsePolymorphicEventSingle(rd io.Reader, readBuf *bytes.Reader) (*model.PolymorphicEvent, error) {
	var byteLen [8]byte
	n, err := io.ReadFull(rd, byteLen[:])
	if err != nil {
		if err == io.EOF {
			return nil, nil
		}
		return nil, errors.Trace(err)
	}
	if n < 8 {
		return nil, errors.Errorf("invalid length data %s, read %d bytes", byteLen, n)
	}
	dataLen := int(binary.BigEndian.Uint64(byteLen[:]))

	data := make([]byte, dataLen)
	n, err = io.ReadFull(rd, data)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if n != dataLen {
		return nil, errors.Errorf("truncated data %s n: %d dataLen: %d", data, n, dataLen)
	}

	readBuf.Reset(data)
	ev := &model.PolymorphicEvent{}
	err = gob.NewDecoder(readBuf).Decode(ev)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &model.PolymorphicEvent{}, nil
}

func parseMsgpackPolymorphicEventSingle(rd io.Reader, readBuf *bytes.Reader) (*model.PolymorphicEvent, error) {
	var byteLen [8]byte
	n, err := io.ReadFull(rd, byteLen[:])
	if err != nil {
		if err == io.EOF {
			return nil, nil
		}
		return nil, errors.Trace(err)
	}
	if n < 8 {
		return nil, errors.Errorf("invalid length data %s, read %d bytes", byteLen, n)
	}
	dataLen := int(binary.BigEndian.Uint64(byteLen[:]))

	data := make([]byte, dataLen)
	n, err = io.ReadFull(rd, data)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if n != dataLen {
		return nil, errors.Errorf("truncated data %s n: %d dataLen: %d", data, n, dataLen)
	}

	readBuf.Reset(data)
	ev := &model.PolymorphicEvent{}

	err = msgpack.NewDecoder(readBuf).Decode(ev)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &model.PolymorphicEvent{}, nil
}

var defaultBuf = make([]byte, defaultBufSize)

func generatePolymorphicEvent() *model.PolymorphicEvent {
	randUint64 := rand.Uint64()
	key := defaultBuf[:rand.Int()%defaultBufSize]
	value := defaultBuf[:rand.Int()%defaultBufSize]
	return &model.PolymorphicEvent{StartTs: randUint64, CRTs: randUint64, RawKV: &model.RawKVEntry{StartTs: randUint64, CRTs: randUint64, Key: key, Value: value}, Row: &model.RowChangedEvent{}}
}

func generatePolymorphicEvents(sz int) []*model.PolymorphicEvent {
	res := make([]*model.PolymorphicEvent, 0, sz)
	for i := 0; i < sz; i++ {
		res = append(res, generatePolymorphicEvent())
	}
	return res
}

var data []*model.PolymorphicEvent

// flushEventsToFile writes a slice of model.PolymorphicEvent to a given file in sequence
func writeEventsToFile(fullpath string, entries []*model.PolymorphicEvent, encodeType string) (int, error) {
	if len(entries) == 0 {
		return 0, nil
	}
	var err error
	buf := new(bytes.Buffer)
	dataBuf := new(bytes.Buffer)
	var dataLen [8]byte
	for _, entry := range entries {
		dataBuf.Reset()
		switch encodeType {
		case "gob":
			err = gob.NewEncoder(dataBuf).Encode(entry)
		case "msgpack":
			err = msgpack.NewEncoder(dataBuf).Encode(entry)
		}
		if err != nil {
			return 0, errors.Trace(err)
		}
		binary.BigEndian.PutUint64(dataLen[:], uint64(dataBuf.Len()))
		buf.Write(dataLen[:])
		buf.Write(dataBuf.Bytes())
	}
	if buf.Len() == 0 {
		return 0, nil
	}
	f, err := os.OpenFile(fullpath, os.O_TRUNC|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return 0, errors.Trace(err)
	}
	w := bufio.NewWriter(f)
	_, err = w.Write(buf.Bytes())
	if err != nil {
		return 0, errors.Trace(err)
	}
	err = w.Flush()
	if err != nil {
		return 0, errors.Trace(err)
	}
	return buf.Len(), nil
}

func baseBenchmark(b *testing.B, suffix string, getReader func(file *os.File) io.Reader, exec func(io.Reader, *bytes.Reader) (*model.PolymorphicEvent, error)) {
	fds := make([]*os.File, 0, FileNum)
	for i := 0; i < FileNum; i++ {
		fileName := genDataPath(EventNumber, EventSize, i, suffix)
		fd, err := os.Open(fileName)
		if err != nil {
			panic(err)
		}
		fds = append(fds, fd)
	}
	var readBuf bytes.Reader

	for j := 0; j < b.N; j++ {
		readers := make([]io.Reader, 0, FileNum)
		for i := 0; i < FileNum; i++ {
			readers = append(readers, getReader(fds[i]))
		}
	Loop:
		for {
			for i := 0; i < FileNum; i++ {
				if ev, err := exec(readers[i], &readBuf); ev != nil && err == nil {

				} else {
					break Loop
				}
			}
		}
	}
	for i := 0; i < FileNum; i++ {
		fds[i].Close()
	}
}

func BenchmarkReadBuf4KSingle(b *testing.B) {
	baseBenchmark(b, "gob", func(file *os.File) io.Reader {
		_, _ = file.Seek(0, io.SeekStart)
		return bufio.NewReaderSize(file, 1024*4)
	}, readPolymorphicEventSingle)
}

func BenchmarkReadBuf8KSingle(b *testing.B) {
	baseBenchmark(b, "gob", func(file *os.File) io.Reader {
		_, _ = file.Seek(0, io.SeekStart)
		return bufio.NewReaderSize(file, 1024*8)
	}, readPolymorphicEventSingle)
}

func BenchmarkReadBuf512KSingle(b *testing.B) {
	baseBenchmark(b, "gob", func(file *os.File) io.Reader {
		_, _ = file.Seek(0, io.SeekStart)
		return bufio.NewReaderSize(file, 1024*512)
	}, readPolymorphicEventSingle)
}

func BenchmarkReadBuf4MSingle(b *testing.B) {
	baseBenchmark(b, "gob", func(file *os.File) io.Reader {
		_, _ = file.Seek(0, io.SeekStart)
		return bufio.NewReaderSize(file, 1024*1024*4)
	}, readPolymorphicEventSingle)
}

func BenchmarkReadBuf16MSingle(b *testing.B) {
	baseBenchmark(b, "gob", func(file *os.File) io.Reader {
		_, _ = file.Seek(0, io.SeekStart)
		return bufio.NewReaderSize(file, 1024*1024*16)
	}, readPolymorphicEventSingle)
}

func BenchmarkReadBufMmapSingle(b *testing.B) {
	baseBenchmark(b, "gob", func(file *os.File) io.Reader {
		res, _ := NewMmapReader(file.Name())
		return res
	}, readPolymorphicEventSingle)
}

func BenchmarkGobParseBuf4KSingle(b *testing.B) {
	baseBenchmark(b, "gob", func(file *os.File) io.Reader {
		_, _ = file.Seek(0, io.SeekStart)
		return bufio.NewReaderSize(file, 1024*4)
	}, parsePolymorphicEventSingle)
}

func BenchmarkGobParseBuf8KSingle(b *testing.B) {
	baseBenchmark(b, "gob", func(file *os.File) io.Reader {
		_, _ = file.Seek(0, io.SeekStart)
		return bufio.NewReaderSize(file, 1024*8)
	}, parsePolymorphicEventSingle)
}

func BenchmarkGobParseBuf512KSingle(b *testing.B) {
	baseBenchmark(b, "gob", func(file *os.File) io.Reader {
		_, _ = file.Seek(0, io.SeekStart)
		return bufio.NewReaderSize(file, 1024*512)
	}, parsePolymorphicEventSingle)
}

func BenchmarkGobParseBuf4MSingle(b *testing.B) {
	baseBenchmark(b, "gob", func(file *os.File) io.Reader {
		_, _ = file.Seek(0, io.SeekStart)
		return bufio.NewReaderSize(file, 1024*1024*4)
	}, parsePolymorphicEventSingle)
}

func BenchmarkGobParseBuf16MSingle(b *testing.B) {
	baseBenchmark(b, "gob", func(file *os.File) io.Reader {
		_, _ = file.Seek(0, io.SeekStart)
		return bufio.NewReaderSize(file, 1024*1024*16)
	}, parsePolymorphicEventSingle)
}

func BenchmarkGobParseBufMmapSingle(b *testing.B) {
	baseBenchmark(b, "gob", func(file *os.File) io.Reader {
		res, _ := NewMmapReader(file.Name())
		return res
	}, parsePolymorphicEventSingle)
}

func BenchmarkMsgpackParseBuf4KSingle(b *testing.B) {
	baseBenchmark(b, "msgpack", func(file *os.File) io.Reader {
		_, _ = file.Seek(0, io.SeekStart)
		return bufio.NewReaderSize(file, 1024*4)
	}, parseMsgpackPolymorphicEventSingle)
}

func BenchmarkMsgpackParseBuf8KSingle(b *testing.B) {
	baseBenchmark(b, "msgpack", func(file *os.File) io.Reader {
		_, _ = file.Seek(0, io.SeekStart)
		return bufio.NewReaderSize(file, 1024*8)
	}, parseMsgpackPolymorphicEventSingle)
}

func BenchmarkMsgpackParseBuf512KSingle(b *testing.B) {
	baseBenchmark(b, "msgpack", func(file *os.File) io.Reader {
		_, _ = file.Seek(0, io.SeekStart)
		return bufio.NewReaderSize(file, 1024*512)
	}, parseMsgpackPolymorphicEventSingle)
}

func BenchmarkMsgpackParseBuf4MSingle(b *testing.B) {
	baseBenchmark(b, "msgpack", func(file *os.File) io.Reader {
		_, _ = file.Seek(0, io.SeekStart)
		return bufio.NewReaderSize(file, 1024*1024*4)
	}, parseMsgpackPolymorphicEventSingle)
}

func BenchmarkMsgpackParseBuf16MSingle(b *testing.B) {
	baseBenchmark(b, "msgpack", func(file *os.File) io.Reader {
		_, _ = file.Seek(0, io.SeekStart)
		return bufio.NewReaderSize(file, 1024*1024*16)
	}, parseMsgpackPolymorphicEventSingle)
}

func BenchmarkMsgpackParseBufMmapSingle(b *testing.B) {
	baseBenchmark(b, "msgpack", func(file *os.File) io.Reader {
		res, _ := NewMmapReader(file.Name())
		return res
	}, parseMsgpackPolymorphicEventSingle)
}
