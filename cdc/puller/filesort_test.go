package puller

import (
	`bufio`
	`bytes`
	`encoding/binary`
	`encoding/gob`
	`io`
	`log`
	`math/rand`
	`os`
	`strconv`
	`testing`

	`github.com/pingcap/errors`

	`github.com/pingcap/ticdc/cdc/model`
)

const startBufSize = 4096 // Size of initial allocation for buffer.

func readPolymorphicEventBatch(rd *bufio.Reader, readBuf *bytes.Reader, batchSize int) ([]*model.PolymorphicEvent, error) {
	var readSize int
	var byteLen [8]byte
	buf := make([]byte, 128)
	evs := make([]*model.PolymorphicEvent, 0, batchSize/128)

	getBuf := func(dataLen int) []byte {
		if dataLen > len(buf) {
			buf = make([]byte, dataLen*2) // append(buf[:cap(buf)], make([]byte, dataLen-cap(buf))...)
		}
		return buf[:dataLen]
	}
	for readSize < batchSize {
		n, err := io.ReadFull(rd, byteLen[:])
		if err != nil {
			if err == io.EOF {
				return evs, nil
			}
			return nil, errors.Trace(err)
		}
		if n < 8 {
			return nil, errors.Errorf("invalid length data %s, read %d bytes", byteLen, n)
		}
		dataLen := int(binary.BigEndian.Uint64(byteLen[:]))
		data := getBuf(dataLen)
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
		//evs = append(evs, ev)
		readSize += 8 + dataLen
	}
	return evs, nil
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

const defaultBufSize = 512
const dataDir = "/tmp/"

var defaultBuf = make([]byte, defaultBufSize)

func generatePolymorphicEvent() *model.PolymorphicEvent {
	randUint64 := rand.Uint64()
	key := defaultBuf[:rand.Int()%defaultBufSize]
	value := defaultBuf[:rand.Int()%defaultBufSize]
	return &model.PolymorphicEvent{StartTs: randUint64, CRTs: randUint64, RawKV: &model.RawKVEntry{StartTs: randUint64, CRTs: randUint64, Key: key, Value: value}}
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
func writeEventsToFile(fullpath string, entries []*model.PolymorphicEvent) (int, error) {
	if len(entries) == 0 {
		return 0, nil
	}
	var err error
	buf := new(bytes.Buffer)
	dataBuf := new(bytes.Buffer)
	var dataLen [8]byte
	for _, entry := range entries {
		dataBuf.Reset()
		err = gob.NewEncoder(dataBuf).Encode(entry)
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

func prepareData(sz int, fileNum int) {
	data = generatePolymorphicEvents(sz)
	for i := 0; i < fileNum; i++ {
		fileName := dataDir + "file-sort-" + strconv.FormatInt(int64(sz), 10) + "-" + strconv.FormatInt(int64(i), 10)
		if _, err := os.Stat(fileName); os.IsNotExist(err) {
			_, err := writeEventsToFile(fileName, data)
			if err != nil {
				log.Printf("write data to file %s", fileName)
			}
		}
	}
}

const EventNumbers = 10000
const FileNum = 10

func init() {
	prepareData(EventNumbers, FileNum)
}

func BenchmarkBuf8KSingle(b *testing.B) {

	fds := make([]*os.File, 0, FileNum)
	for i := 0; i < FileNum; i++ {
		fileName := dataDir + "file-sort-" + strconv.FormatInt(int64(EventNumbers), 10) + "-" + strconv.FormatInt(int64(i), 10)
		fd, err := os.Open(fileName)
		if err != nil {
			panic(err)
		}
		fds = append(fds, fd)

	}
	var readBuf bytes.Reader

Loop:
	for j := 0; j < b.N; j++ {
		readers := make([]*bufio.Reader, 0, FileNum)
		for i := 0; i < FileNum; i++ {
			readers = append(readers, bufio.NewReaderSize(fds[i], 1024*8))
		}
		for {
			for i := 0; i < FileNum; i++ {
				if ev, err := readPolymorphicEventSingle(readers[i], &readBuf); ev != nil && err == nil {

				} else {
					continue Loop
				}
			}
		}

	}
	for i := 0; i < FileNum; i++ {
		fds[i].Close()
	}
}

func BenchmarkBuf512KSingle(b *testing.B) {
	fds := make([]*os.File, 0, FileNum)
	for i := 0; i < FileNum; i++ {
		fileName := dataDir + "file-sort-" + strconv.FormatInt(int64(EventNumbers), 10) + "-" + strconv.FormatInt(int64(i), 10)
		fd, err := os.Open(fileName)
		if err != nil {
			panic(err)
		}
		fds = append(fds, fd)

	}
	var readBuf bytes.Reader

Loop:
	for j := 0; j < b.N; j++ {
		readers := make([]*bufio.Reader, 0, FileNum)
		for i := 0; i < FileNum; i++ {
			readers = append(readers, bufio.NewReaderSize(fds[i], 1024*512))
		}
		for {
			for i := 0; i < FileNum; i++ {
				if ev, err := readPolymorphicEventSingle(readers[i], &readBuf); ev != nil && err == nil {

				} else {
					continue Loop
				}
			}
		}
	}
	for i := 0; i < FileNum; i++ {
		fds[i].Close()
	}
}

func BenchmarkBuf4KSingle(b *testing.B) {
	fds := make([]*os.File, 0, FileNum)
	for i := 0; i < FileNum; i++ {
		fileName := dataDir + "file-sort-" + strconv.FormatInt(int64(EventNumbers), 10) + "-" + strconv.FormatInt(int64(i), 10)
		fd, err := os.Open(fileName)
		if err != nil {
			panic(err)
		}
		fds = append(fds, fd)

	}
	var readBuf bytes.Reader

Loop:
	for j := 0; j < b.N; j++ {
		readers := make([]*bufio.Reader, 0, FileNum)
		for i := 0; i < FileNum; i++ {
			readers = append(readers, bufio.NewReaderSize(fds[i], 1024*4))
		}
		for {
			for i := 0; i < FileNum; i++ {
				if ev, err := readPolymorphicEventSingle(readers[i], &readBuf); ev != nil && err == nil {

				} else {
					continue Loop
				}
			}
		}
	}
	for i := 0; i < FileNum; i++ {
		fds[i].Close()
	}
}

func BenchmarkBuf4MSingle(b *testing.B) {
	fds := make([]*os.File, 0, FileNum)
	for i := 0; i < FileNum; i++ {
		fileName := dataDir + "file-sort-" + strconv.FormatInt(int64(EventNumbers), 10) + "-" + strconv.FormatInt(int64(i), 10)
		fd, err := os.Open(fileName)
		if err != nil {
			panic(err)
		}
		fds = append(fds, fd)

	}
	var readBuf bytes.Reader

Loop:
	for j := 0; j < b.N; j++ {
		readers := make([]*bufio.Reader, 0, FileNum)
		for i := 0; i < FileNum; i++ {
			readers = append(readers, bufio.NewReaderSize(fds[i], 1024*1024*4))
		}
		for {
			for i := 0; i < FileNum; i++ {
				if ev, err := readPolymorphicEventSingle(readers[i], &readBuf); ev != nil && err == nil {

				} else {
					continue Loop
				}
			}
		}
	}
	for i := 0; i < FileNum; i++ {
		fds[i].Close()
	}
}

func BenchmarkBuf16MSingle(b *testing.B) {
	fds := make([]*os.File, 0, FileNum)
	for i := 0; i < FileNum; i++ {
		fileName := dataDir + "file-sort-" + strconv.FormatInt(int64(EventNumbers), 10) + "-" + strconv.FormatInt(int64(i), 10)
		fd, err := os.Open(fileName)
		if err != nil {
			panic(err)
		}
		fds = append(fds, fd)

	}
	var readBuf bytes.Reader

Loop:
	for j := 0; j < b.N; j++ {
		readers := make([]*bufio.Reader, 0, FileNum)
		for i := 0; i < FileNum; i++ {
			readers = append(readers, bufio.NewReaderSize(fds[i], 1024*1024*16))
		}
		for {
			for i := 0; i < FileNum; i++ {
				if ev, err := readPolymorphicEventSingle(readers[i], &readBuf); ev != nil && err == nil {

				} else {
					continue Loop
				}
			}
		}
	}
	for i := 0; i < FileNum; i++ {
		fds[i].Close()
	}
}

func BenchmarkBufMmapSingle(b *testing.B) {
	var readBuf bytes.Reader
Loop:
	for j := 0; j < b.N; j++ {
		readers := make([]*MmapReader, 0, FileNum)
		for i := 0; i < FileNum; i++ {
			fileName := dataDir + "file-sort-" + strconv.FormatInt(int64(EventNumbers), 10) + "-" + strconv.FormatInt(int64(i), 10)
			reader, err := NewMmapReader(fileName)
			if err != nil {
				panic(err)
			}
			readers = append(readers, reader)
		}
		for {
			for i := 0; i < FileNum; i++ {
				if ev, err := readPolymorphicEventSingle(readers[i], &readBuf); ev != nil && err == nil {

				} else {
					continue Loop
				}
			}
		}
	}
}

const L2Cache = 256 * 1024

//func BenchmarkBuf4KBatch256K(b *testing.B) {
//	fileName := dataDir + "file-sort-" + strconv.FormatInt(int64(EventNumbers), 10)
//	for i := 0; i < b.N; i++ {
//		fd, err := os.Open(fileName)
//		if err != nil {
//			panic(err)
//		}
//		rd := bufio.NewReaderSize(fd, 1024*4)
//		var readBuf bytes.Reader
//		for evs, err := readPolymorphicEventBatch(rd, &readBuf, L2Cache); len(evs) != 0 && err == nil; evs, err = readPolymorphicEventBatch(rd, &readBuf, L2Cache) {
//
//		}
//		_ = fd.Close()
//	}
//}
//
//func BenchmarkBuf8KBatch(b *testing.B) {
//	fileName := dataDir + "file-sort-" + strconv.FormatInt(int64(EventNumbers), 10)
//	for i := 0; i < b.N; i++ {
//		fd, err := os.Open(fileName)
//		if err != nil {
//			panic(err)
//		}
//		rd := bufio.NewReaderSize(fd, 1024*8)
//		var readBuf bytes.Reader
//		for evs, err := readPolymorphicEventBatch(rd, &readBuf, L2Cache); len(evs) != 0 && err == nil; evs, err = readPolymorphicEventBatch(rd, &readBuf, L2Cache) {
//
//		}
//		_ = fd.Close()
//	}
//}
//
//func BenchmarkBuf512KBatch(b *testing.B) {
//	fileName := dataDir + "file-sort-" + strconv.FormatInt(int64(EventNumbers), 10)
//	for i := 0; i < b.N; i++ {
//		fd, err := os.Open(fileName)
//		if err != nil {
//			panic(err)
//		}
//		rd := bufio.NewReaderSize(fd, 1024*512)
//		var readBuf bytes.Reader
//		for evs, err := readPolymorphicEventBatch(rd, &readBuf, L2Cache); len(evs) != 0 && err == nil; evs, err = readPolymorphicEventBatch(rd, &readBuf, L2Cache) {
//
//		}
//		_ = fd.Close()
//	}
//}

func BenchmarkBuf4MBatch(b *testing.B) {
	fds := make([]*os.File, 0, FileNum)
	for i := 0; i < FileNum; i++ {
		fileName := dataDir + "file-sort-" + strconv.FormatInt(int64(EventNumbers), 10) + "-" + strconv.FormatInt(int64(i), 10)
		fd, err := os.Open(fileName)
		if err != nil {
			panic(err)
		}
		fds = append(fds, fd)

	}
	var readBuf bytes.Reader

Loop:
	for j := 0; j < b.N; j++ {
		readers := make([]*bufio.Reader, 0, FileNum)
		for i := 0; i < FileNum; i++ {
			readers = append(readers, bufio.NewReaderSize(fds[i], 1024*512))
		}
		for {
			for i := 0; i < FileNum; i++ {
				if evs, err := readPolymorphicEventBatch(readers[i], &readBuf, 1024*512); len(evs) != 0 && err == nil {

				} else {
					continue Loop
				}
			}
		}
	}
	for i := 0; i < FileNum; i++ {
		fds[i].Close()
	}
}
