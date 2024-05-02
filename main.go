package main

import "fmt"

func main() {
	filename := "test-data/user-1000.journal"
	reader, err := newReader(filename)

	if err != nil {
		panic(err)
	}

	go reader.readAll()

	for log := range reader.data {
		fmt.Printf("\n\n")
		for key, value := range log.attributes {
			fmt.Printf("%v=%v\n", key, value)
		}
	}
}
