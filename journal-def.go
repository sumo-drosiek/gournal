package main

import (
	"bytes"
	"encoding/binary"

	"github.com/klauspost/compress/zstd"
	"github.com/pierrec/lz4"
	"github.com/ulikunitz/xz"
)

// This implementation base on https://systemd.io/JOURNAL_FILE_FORMAT/

const (
	// Definitions for Header incompatible_flags
	HEADER_INCOMPATIBLE_COMPRESSED_XZ   = 1 << 0
	HEADER_INCOMPATIBLE_COMPRESSED_LZ4  = 1 << 1
	HEADER_INCOMPATIBLE_KEYED_HASH      = 1 << 2
	HEADER_INCOMPATIBLE_COMPRESSED_ZSTD = 1 << 3
	HEADER_INCOMPATIBLE_COMPACT         = 1 << 4

	// Definitions for Header compatible_flags
	HEADER_COMPATIBLE_SEALED             = 1 << 0
	HEADER_COMPATIBLE_TAIL_ENTRY_BOOT_ID = 1 << 1

	// Definitions for Header states
	STATE_OFFLINE  = 0
	STATE_ONLINE   = 1
	STATE_ARCHIVED = 2

	// Definitions for ObjectHeader flags
	OBJECT_COMPRESSED_XZ   = 1 << 0
	OBJECT_COMPRESSED_LZ4  = 1 << 1
	OBJECT_COMPRESSED_ZSTD = 1 << 2

	// Maximum size for Header
	// uint8_t -> 1
	// le32_t -> 4
	// sd_id128_t -> 16*1
	// le64_t -> 8
	HEADER_MAX_SIZE = 16*1 + 2*4 + 4*16*1 + 24*8

	TAG_LENGTH = 256 / 8

	// Definitions for object types
	OBJECT_UNUSED           = 0
	OBJECT_DATA             = 1
	OBJECT_FIELD            = 2
	OBJECT_ENTRY            = 3
	OBJECT_DATA_HASH_TABLE  = 4
	OBJECT_FIELD_HASH_TABLE = 5
	OBJECT_ENTRY_ARRAY      = 6
	OBJECT_TAG              = 7

	// ObjectHeader size
	// uint8_t -> 1
	// le64_t -> 8
	OBJECT_HEADER_SIZE = 8*1 + 1*8

	ATTRIBUTE_CURSOR              = "__CURSOR"
	ATTRIBUTE_REALTIME_TIMESTAMP  = "__REALTIME_TIMESTAMP"
	ATTRIBUTE_MONOTONIC_TIMESTAMP = "__MONOTONIC_TIMESTAMP"
)

// le32 converts [4]byte to uint32
func le32(data [4]byte) uint32 {
	return binary.LittleEndian.Uint32(data[:])
}

// le64 converts [8]byte to uint32
func le64(data [8]byte) uint64 {
	return binary.LittleEndian.Uint64(data[:])
}

// Definition of Header type
// rel: https://systemd.io/JOURNAL_FILE_FORMAT/#header
type Header struct {
	signature               [8]uint8 // uint8_t[8] "LPKSHHRH"
	compatible_flags        uint32   // le32_t
	incompatible_flags      uint32   // le32_t
	state                   uint8    // uint8_t
	reserved                [7]uint8 // uint8_t[7]
	file_id                 [16]byte // sd_id128_t
	machine_id              [16]byte // sd_id128_t
	tail_entry_boot_id      [16]byte // sd_id128_t
	seqnum_id               [16]byte // sd_id128_t
	header_size             uint64   // le64_t
	arena_size              uint64   // le64_t
	data_hash_table_offset  uint64   // le64_t
	data_hash_table_size    uint64   // le64_t
	field_hash_table_offset uint64   // le64_t
	field_hash_table_size   uint64   // le64_t
	tail_object_offset      uint64   // le64_t
	n_objects               uint64   // le64_t
	n_entries               uint64   // le64_t
	tail_entry_seqnum       uint64   // le64_t
	head_entry_seqnum       uint64   // le64_t
	entry_array_offset      uint64   // le64_t
	head_entry_realtime     uint64   // le64_t
	tail_entry_realtime     uint64   // le64_t
	tail_entry_monotonic    uint64   // le64_t
	// /* Added in 187 */
	n_data   uint64 // le64_t
	n_fields uint64 // le64_t
	// /* Added in 189 */
	n_tags         uint64 // le64_t
	n_entry_arrays uint64 // le64_t
	// /* Added in 246 */
	data_hash_chain_depth  uint64 // le64_t
	field_hash_chain_depth uint64 // le64_t
	// /* Added in 252 */
	tail_entry_array_offset    uint64 // le64_t
	tail_entry_array_n_entries uint64 // le64_t
	// /* Added in 254 */
	tail_entry_offset uint64 // le64_t
}

// newHeader creates Header out of byte array
func newHeader(data []byte) *Header {
	hu := Header{
		signature:               ([8]byte)(data[0:8]),
		compatible_flags:        le32(([4]byte)(data[8:12])),
		incompatible_flags:      le32(([4]byte)(data[12:16])),
		state:                   data[16],
		reserved:                ([7]byte)(data[17:24]),
		file_id:                 ([16]byte)(data[24:40]),
		machine_id:              ([16]byte)(data[40:56]),
		tail_entry_boot_id:      ([16]byte)(data[56:72]),
		seqnum_id:               ([16]byte)(data[72:88]),
		header_size:             le64(([8]byte)(data[88:96])),
		arena_size:              le64(([8]byte)(data[96:104])),
		data_hash_table_offset:  le64(([8]byte)(data[104:112])),
		data_hash_table_size:    le64(([8]byte)(data[112:120])),
		field_hash_table_offset: le64(([8]byte)(data[120:128])),
		field_hash_table_size:   le64(([8]byte)(data[128:136])),
		tail_object_offset:      le64(([8]byte)(data[136:144])),
		n_objects:               le64(([8]byte)(data[144:152])),
		n_entries:               le64(([8]byte)(data[152:160])),
		tail_entry_seqnum:       le64(([8]byte)(data[160:168])),
		head_entry_seqnum:       le64(([8]byte)(data[168:176])),
		entry_array_offset:      le64(([8]byte)(data[176:184])),
		head_entry_realtime:     le64(([8]byte)(data[184:192])),
		tail_entry_realtime:     le64(([8]byte)(data[192:200])),
		tail_entry_monotonic:    le64(([8]byte)(data[200:208])),
	}

	// Added in 187
	if hu.header_size > 208 {
		hu.n_data = le64(([8]byte)(data[208:216]))
		hu.n_fields = le64(([8]byte)(data[216:224]))
	}

	// Added in 189
	if hu.header_size > 224 {
		hu.n_tags = le64(([8]byte)(data[224:232]))
		hu.n_entry_arrays = le64(([8]byte)(data[232:240]))
	}

	// Added in 246
	if hu.header_size > 240 {
		hu.data_hash_chain_depth = le64(([8]byte)(data[240:248]))
		hu.field_hash_chain_depth = le64(([8]byte)(data[248:256]))
	}

	// Added in 252
	if hu.header_size > 256 {
		hu.tail_entry_array_offset = le64(([8]byte)(data[256:264]))
		hu.tail_entry_array_n_entries = le64(([8]byte)(data[264:272]))
	}

	// Added in 254
	if hu.header_size > 272 {
		hu.tail_entry_offset = le64(([8]byte)(data[272:280]))
	}

	return &hu
}

// isCompact returns true if HEADER_INCOMPATIBLE_COMPACT flag is enable
func (hu Header) isCompact() bool {
	return hu.incompatible_flags&HEADER_INCOMPATIBLE_COMPACT > 0
}

// isOnline returns true if state is online
func (hu Header) isOnline() bool {
	return hu.state&STATE_ONLINE > 0
}

// isOnline returns true if state is offline
func (hu Header) isOffline() bool {
	return hu.state&STATE_OFFLINE > 0
}

// isOnline returns true if state is archived
func (hu Header) isArchived() bool {
	return hu.state&STATE_ARCHIVED > 0
}

// Definition of ObjectHeader type
// rel: https://systemd.io/JOURNAL_FILE_FORMAT/#objects
type ObjectHeader struct {
	objectType uint8    // uint8_t
	flags      uint8    // uint8_t
	reserved   [6]uint8 // uint8_t[6]
	size       uint64   // le64_t
	payload    []uint8  // uint8_t[]
}

// newObjectHeader creates ObjectHeader out of byte array
// !!!It doesn't set the payload. Use setPayload to set it!!!
func newObjectHeader(data []byte) *ObjectHeader {
	oh := ObjectHeader{
		objectType: data[0],
		flags:      data[1],
		reserved:   ([6]byte)(data[2:8]),
		size:       le64(([8]byte)(data[8:16])),
	}

	return &oh
}

// setPayload sets payload for ObjectHeader
func (oh *ObjectHeader) setPayload(payload []byte) {
	oh.payload = payload
}

// payloadSize computes payload size as diff of declared size and header size
func (oh *ObjectHeader) payloadSize() int {
	return int(oh.size) - OBJECT_HEADER_SIZE
}

// DataHashTable returns EntryArray object out of the ObjectHeader object
func (oh *ObjectHeader) EntryArray(incompatible_compact bool) *EntryArray {
	do := EntryArray{
		ObjectHeader: oh,

		next_entry_array_offset: le64(([8]byte)(oh.payload[0:8])),
	}

	regularItems := []uint64{}
	compactItems := []uint32{}

	ptr := 8
	if incompatible_compact {
		for {
			if ptr == len(oh.payload) {
				break
			}
			compactItems = append(compactItems, le32(([4]byte)(oh.payload[ptr:ptr+4])))
			ptr += 4
		}
	} else {
		for {
			if ptr == len(oh.payload) {
				break
			}
			regularItems = append(regularItems, le64(([8]byte)(oh.payload[ptr:ptr+8])))
			ptr += 8
		}
	}

	do.compactItems = compactItems
	do.regularItems = regularItems

	return &do
}

// DataHashTable returns Entry object out of the ObjectHeader object
func (oh *ObjectHeader) Entry(incompatible_compact bool) *Entry {
	eo := Entry{
		ObjectHeader: oh,

		seqnum:    le64(([8]byte)(oh.payload[0:8])),
		realtime:  le64(([8]byte)(oh.payload[8:16])),
		monotonic: le64(([8]byte)(oh.payload[16:24])),
		boot_id:   ([16]byte)(oh.payload[24:40]),
		xor_hash:  le64(([8]byte)(oh.payload[40:48])),
	}

	regularItems := []regularEntryItem{}
	compactItems := []compactEntryItem{}

	ptr := 48
	if incompatible_compact {
		for {
			if ptr == len(oh.payload) {
				break
			}
			compactItems = append(compactItems, compactEntryItem{
				object_offset: le32(([4]byte)(oh.payload[ptr : ptr+4])),
			})
			ptr += 4
		}
	} else {
		for {
			if ptr == len(oh.payload) {
				break
			}
			regularItems = append(regularItems, regularEntryItem{
				object_offset: le64(([8]byte)(oh.payload[ptr : ptr+8])),
				hash:          le64(([8]byte)(oh.payload[ptr+8 : ptr+16])),
			})
			ptr += 16
		}
	}

	eo.itemsRegular = regularItems
	eo.itemsCompact = compactItems

	return &eo
}

// DataHashTable returns Data object out of the ObjectHeader object
func (oh *ObjectHeader) Data(incompatibleCompact bool) *Data {
	do := Data{
		ObjectHeader:       oh,
		hash:               le64(([8]byte)(oh.payload[0:8])),
		next_hash_offset:   le64(([8]byte)(oh.payload[8:16])),
		next_field_offset:  le64(([8]byte)(oh.payload[16:24])),
		entry_offset:       le64(([8]byte)(oh.payload[24:32])),
		entry_array_offset: le64(([8]byte)(oh.payload[32:40])),
		n_entries:          le64(([8]byte)(oh.payload[40:48])),
	}

	if incompatibleCompact {
		do.tail_entry_array_offset = le32(([4]byte)(oh.payload[48:52]))
		do.tail_entry_array_n_entries = le32(([4]byte)(oh.payload[52:56]))
		do.payload = oh.payload[56:]
	} else {
		do.payload = oh.payload[48:]
	}

	return &do
}

// DataHashTable returns HashTable object out of the ObjectHeader object
func (oh *ObjectHeader) DataHashTable() *HashTable {
	hashItems := []HashItem{}

	ht := HashTable{
		ObjectHeader: oh,
	}

	ptr := 0
	for {
		if ptr == len(oh.payload) {
			break
		}
		hashItems = append(hashItems, HashItem{
			head_hash_offset: le64(([8]byte)(oh.payload[ptr : ptr+8])),
			tail_hash_offset: le64(([8]byte)(oh.payload[ptr+8 : ptr+16])),
		})
		ptr += 16
	}

	ht.items = hashItems

	return &ht
}

// definition of Data type
// rel: https://systemd.io/JOURNAL_FILE_FORMAT/#data-objects
type Data struct {
	*ObjectHeader

	hash               uint64 // le64_t
	next_hash_offset   uint64 // le64_t
	next_field_offset  uint64 // le64_t
	entry_offset       uint64 // le64_t /* the first array entry we store inline */
	entry_array_offset uint64 // le64_t
	n_entries          uint64 // le64_t

	// If HEADER_INCOMPATIBLE_COMPACT is set
	tail_entry_array_offset    uint32 // le32_t
	tail_entry_array_n_entries uint32 // le32_t

	payload []uint8 // uint8_t[]
}

// getPayloadKeyValue returns payload as key and value strings
// it handles compressed payload
func (so Data) getPayloadKeyValue() (string, string, error) {
	var payload []uint8

	switch true {
	case so.flags&OBJECT_COMPRESSED_XZ > 0:
		// decompress xz payload
		r, err := xz.NewReader(bytes.NewReader(so.payload))
		if err != nil {
			return "", "", err
		}
		_, err = r.Read(payload)
		if err != nil {
			return "", "", err
		}
	case so.flags&OBJECT_COMPRESSED_LZ4 > 0:
		// decompress lz4 payload
		r := lz4.NewReader(bytes.NewReader(so.payload))
		_, err := r.Read(payload)
		if err != nil {
			return "", "", err
		}
	case so.flags&OBJECT_COMPRESSED_ZSTD > 0:
		// decompress zstd payload
		r, err := zstd.NewReader(bytes.NewReader([]byte{}))
		if err != nil {
			return "", "", err
		}
		payload, err = r.DecodeAll(so.payload, nil)
		if err != nil {
			return "", "", err
		}
	default:
		payload = so.payload
	}

	// Split payload by first `=`
	for i := 0; ; i++ {
		if payload[i] == '=' {
			return string(payload[:i]), string(payload[i+1:]), nil
		}
	}
}

// definition of Field type
// rel: https://systemd.io/JOURNAL_FILE_FORMAT/#field-objects
type Field struct {
	*ObjectHeader

	hash             uint64 // le64_t
	next_hash_offset uint64 // le64_t
	head_data_offset uint64 // le64_t

	payload []uint8 // uint8_t[]
}

// definition of helper structure for regular items
type regularEntryItem struct {
	object_offset uint64 // le64_t
	hash          uint64 // le64_t
}

// definition of helper structure for compact items
type compactEntryItem struct {
	object_offset uint32 // le32_t
}

// definition of Entry type
// rel: https://systemd.io/JOURNAL_FILE_FORMAT/#entry-objects
type Entry struct {
	*ObjectHeader

	seqnum    uint64   // le64_t
	realtime  uint64   // le64_t
	monotonic uint64   // le64_t
	boot_id   [16]byte // sd_id128_t
	xor_hash  uint64   // le64_t

	// for not HEADER_INCOMPATIBLE_COMPACT
	itemsRegular []regularEntryItem

	// for HEADER_INCOMPATIBLE_COMPACT
	itemsCompact []compactEntryItem
}

// items returns EntryArray items in format of regular items
func (eo *Entry) items() []regularEntryItem {
	if len(eo.itemsRegular) > 0 {
		return eo.itemsRegular
	}

	items := []regularEntryItem{}
	for _, item := range eo.itemsCompact {
		items = append(items, regularEntryItem{
			object_offset: uint64(item.object_offset),
		})
	}
	return items
}

// definition of HashItem type
// rel: https://systemd.io/JOURNAL_FILE_FORMAT/#hash-table-objects
type HashItem struct {
	head_hash_offset uint64 // le64_t
	tail_hash_offset uint64 // le64_t
}

// definition of DataHashTable type
// rel: https://systemd.io/JOURNAL_FILE_FORMAT/#hash-table-objects
type HashTable struct {
	*ObjectHeader

	items []HashItem // HashItem[]
}

// definition of Entry Array type
// rel: https://systemd.io/JOURNAL_FILE_FORMAT/#entry-array-objects
type EntryArray struct {
	*ObjectHeader

	next_entry_array_offset uint64 // le64_t

	regularItems []uint64 // le64_t[] // if not HEADER_INCOMPATIBLE_COMPACT
	compactItems []uint32 // le32_t[] // for HEADER_INCOMPATIBLE_COMPACT

	countItems int
}

// items returns EntryArray items in format of regularItems
func (eao *EntryArray) items() []uint64 {
	if len(eao.regularItems) > 0 {
		eao.countItems = len(eao.regularItems)
		return eao.regularItems
	}

	items := []uint64{}
	for _, item := range eao.compactItems {
		items = append(items, uint64(item))
	}
	eao.countItems = len(eao.compactItems)
	return items
}

// definition of Tag type
// rel: https://systemd.io/JOURNAL_FILE_FORMAT/#tag-object
type Tag struct {
	*ObjectHeader

	seqnum uint64 // le64_t
	epoch  uint64 // le64_t

	tag [TAG_LENGTH]uint8 // uint8_t[] /* SHA-256 HMAC */
}

// Log is just a key-value map
type Log struct {
	filePath   string
	attributes map[string]string
}
