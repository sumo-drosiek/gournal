package main

import "fmt"

func main() {
	filename := "test-data/user-1000.journal"
	reader, err := newReader(filename)

	if err != nil {
		panic(err)
	}

	// get offset to first EntryArray
	offset := reader.header.entry_array_offset
	logs := []Log{}
main:
	for {
		// get EntryArray
		ea, err := reader.getEntryArray(offset)
		if err != nil {
			panic(err)
		}

		// get list of Entries offset
		entriesOffset := ea.items()

		for _, entryOffset := range entriesOffset {
			// there is nothing more to read
			if entryOffset == 0 {
				break main
			}

			// get Entry
			entry, err := reader.getEntry(entryOffset)
			if err != nil {
				panic(err)
			}

			// get list of Data offset
			dataOffsets := entry.items()
			attributes := map[string]string{}

			for _, dataOffset := range dataOffsets {
				// there is nothing more to read for this Data
				if dataOffset.object_offset == 0 {
					break
				}

				// read Data starting with given offset
				dataObject, err := reader.getData(dataOffset.object_offset)
				if err != nil {
					panic(err)
				}

				// get key value pair of the Data and append to the attributes list
				key, value, err := dataObject.getPayloadKeyValue()
				if err != nil {
					panic(err)
				}
				attributes[key] = value
			}

			// create log out of attributes list and append to Logs list
			logs = append(logs, Log{attributes: attributes})
		}
		// entry array has been readed, so set the offset to the next one
		offset = ea.next_entry_array_offset
	}

	// print logs
	for _, log := range logs {
		fmt.Printf("\n\n")
		for key, value := range log.attributes {
			fmt.Printf("%v=%v\n", key, value)
		}
	}
}
