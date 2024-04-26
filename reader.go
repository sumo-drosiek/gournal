package main

import (
	"errors"
	"os"
)

// Reader object
type Reader struct {
	file   *os.File
	buffer []byte

	header       *Header
	headerBuffer []byte
	compact      bool
}

// newReader creates Reader for the given filename
func newReader(filename string) (*Reader, error) {
	// Open file
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}

	// prepare buffer and read file header
	buffer := make([]byte, HEADER_MAX_SIZE)
	_, err = file.Read(buffer)
	if err != nil {
		return nil, err
	}
	header := newHeader(buffer)

	// check file signature
	if string(header.signature[:]) != "LPKSHHRH" {
		return nil, errors.New("file signature is invalid")
	}

	// return Reader
	return &Reader{
		file:         file,
		buffer:       make([]byte, 1024*1024*1024),
		headerBuffer: make([]byte, OBJECT_HEADER_SIZE),
		header:       header,
		compact:      header.isCompact(),
	}, nil
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

	oh := newObjectHeader(r.headerBuffer)

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
