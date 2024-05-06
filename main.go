package main

import (
	"context"
	"fmt"
)

func main() {
	filename := "test-data/user-1000.journal"
	cursor := "s=69e0bc24292040569344cea3ad97204c;i=1c41;b=6b84ae3ed1114c0b900c8c464e64a015;m=49623cf64;t=616d3ccb25bef;x=c6eb81d0bd51b7a5"
	reader, err := newReader(filename)

	if err != nil {
		panic(err)
	}

	err = reader.goToCursor(cursor)
	if err != nil {
		panic(err)
	}

	go reader.readAll(context.Background())

	for log := range reader.data {
		fmt.Printf("\n\n")
		for key, value := range log.attributes {
			fmt.Printf("%v=%v\n", key, value)
		}
	}
}
