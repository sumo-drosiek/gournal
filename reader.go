package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"
)

// Reader object
type Reader struct {
	file   *os.File
	buffer []byte

	header       *Header
	headerBuffer []byte
	compact      bool

	nextArrayOffset uint64
	nextItemOffset  int

	pollTime time.Duration

	data chan Log
}

// newReader creates Reader for the given filename
func newReader(filename string) (*Reader, error) {
	// Open file
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}

	reader := Reader{
		file:           file,
		buffer:         make([]byte, 1024*1024*1024),
		headerBuffer:   make([]byte, OBJECT_HEADER_SIZE),
		nextItemOffset: 0,
		data:           make(chan Log),
		pollTime:       200 * time.Millisecond,
	}

	err = reader.loadHeader()
	if err != nil {
		return nil, err
	}
	reader.resetOffset()

	// return Reader
	return &reader, nil
}

func (r *Reader) loadHeader() error {
	// prepare buffer and read file header
	buffer := make([]byte, HEADER_MAX_SIZE)
	_, err := r.file.Seek(0, 0)
	if err != nil {
		return err
	}
	_, err = r.file.Read(buffer)
	if err != nil {
		return err
	}
	header, err := newHeader(buffer)

	if err != nil {
		return err
	}

	// check file signature
	if string(header.signature[:]) != "LPKSHHRH" {
		return errors.New("file signature is invalid")
	}

	r.header = header
	r.compact = header.isCompact()

	return nil
}

func (r *Reader) resetOffset() {
	r.nextArrayOffset = r.header.entry_array_offset
}

// getObject reads the object starting with the given offset
func (r *Reader) getObject(offset uint64) (*ObjectHeader, error) {
	// set pointer to given offset
	_, err := r.file.Seek(int64(offset), 0)
	if err != nil {
		return nil, err
	}

	// read ObjectHeader
	read, err := r.file.Read(r.headerBuffer)
	if err != nil {
		return nil, err
	}
	if read < OBJECT_HEADER_SIZE {
		return nil, errors.New("cannot create object header. Not enough data has been read")
	}

	oh, err := newObjectHeader(r.headerBuffer)
	if err != nil {
		return nil, err
	}

	// load payload to the ObjectHeader
	read, err = r.file.Read(r.buffer[:oh.payloadSize()])
	if err != nil {
		return nil, err
	}
	if read < oh.payloadSize() {
		return nil, errors.New("cannot create object payload. Not enough data has been read")
	}
	oh.setPayload(r.buffer[:oh.payloadSize()])

	// return ObjectHeader
	return oh, nil
}

// getData returns Data object starting with given offset
func (r *Reader) getData(offset uint64) (*Data, error) {
	// read object starting with given offset
	oh, err := r.getObject(offset)
	if err != nil {
		return nil, err
	}

	// return Data object
	return oh.Data(r.compact), nil
}

// getEntryArray returns EntryArray object starting with given offset
func (r *Reader) getEntryArray(offset uint64) (*EntryArray, error) {
	// read object starting with given offset
	oh, err := r.getObject(offset)
	if err != nil {
		return nil, err
	}

	// return EntryArray object
	return oh.EntryArray(r.compact), nil
}

// getEntry returns Entry object starting with given offset
func (r *Reader) getEntry(offset uint64) (*Entry, error) {
	oh, err := r.getObject(offset)
	if err != nil {
		return nil, err
	}

	// return EntryArray object
	return oh.Entry(r.compact), nil
}

// get cursor value
func (r *Reader) getCursor(entry *Entry) string {
	return fmt.Sprintf(
		"s=%x;i=%x;b=%x;m=%x;t=%x;x=%x",
		r.header.seqnum_id[:],
		entry.seqnum,
		entry.boot_id[:],
		entry.monotonic,
		entry.realtime,
		entry.xor_hash,
	)
}

// set next entry right after the cursor
func (r *Reader) goToCursor(cursor string) error {
	err := r.loadHeader()
	if err != nil {
		return err
	}
	r.resetOffset()

main:
	for {
		for {
			entry, err := r.getNextEntry()

			if err != nil {
				return err
			}

			if entry == nil {
				break main
			}

			// we just read the entry from the cursor, so next one will be to read
			if r.getCursor(entry) == cursor {
				return nil
			}
		}
	}
	return fmt.Errorf("entry for specified cursor has not been find")
}

// initAttributes returns map containing attributes based on the entry structure
func (r *Reader) initAttributes(entry *Entry) map[string]string {
	return map[string]string{
		ATTRIBUTE_CURSOR:              r.getCursor(entry),
		ATTRIBUTE_REALTIME_TIMESTAMP:  fmt.Sprintf("%d", entry.realtime),
		ATTRIBUTE_MONOTONIC_TIMESTAMP: fmt.Sprintf("%d", entry.monotonic),
	}
}

// readData from specific Entry
func (r *Reader) readData(entry *Entry) (map[string]string, error) {
	// get list of Data offset
	dataOffsets := entry.items()
	attributes := r.initAttributes(entry)

	for _, dataOffset := range dataOffsets {
		// there is nothing more to read for this Data
		if dataOffset.object_offset == 0 {
			break
		}

		// read Data starting with given offset
		dataObject, err := r.getData(dataOffset.object_offset)
		if err != nil {
			return map[string]string{}, err
		}

		// get key value pair of the Data and append to the attributes list
		key, value, err := dataObject.getPayloadKeyValue()
		if err != nil {
			return map[string]string{}, err
		}
		attributes[key] = value
	}

	return attributes, nil
}

// getNextEntry returns next entry in the queue
func (r *Reader) getNextEntry() (*Entry, error) {
	entryArray, err := r.getEntryArray(r.nextArrayOffset)
	if err != nil {
		return nil, err
	}

	entryOffset := entryArray.items()[r.nextItemOffset]

	// return nils if there is nothing to read
	if entryOffset == 0 {
		return nil, nil
	}

	// set pointer to next element
	if r.nextItemOffset == entryArray.countItems-1 {
		r.nextItemOffset = 0
		r.nextArrayOffset = entryArray.next_entry_array_offset
	} else {
		r.nextItemOffset += 1
	}

	// return entry
	return r.getEntry(entryOffset)
}

// readAll reads the data and push it to data channel
func (r *Reader) readAll(ctx context.Context) {
main:
	for {
		for {
			err := r.loadHeader()
			if err != nil {
				panic(err)
			}

			entry, err := r.getNextEntry()

			if err != nil {
				panic(err)
			}

			if entry == nil {
				switch r.header.state {
				// file is rotated, so we do not expect more data
				case STATE_ARCHIVED:
					close(r.data)
					break main
				// wait for database to be in offline state
				case STATE_ONLINE:
					time.Sleep(r.pollTime)
					continue main
				// wait for more data
				default:
					if ctx.Err() != nil {
						close(r.data)
						break main
					}
					time.Sleep(r.pollTime)
					continue main
				}
			}

			attributes, err := r.readData(entry)

			if err != nil {
				// ToDo convert to log or propagate error
				panic(err)
			}

			r.data <- Log{
				attributes: attributes,
			}
		}
	}
}
