package main

import (
	"context"
	"fmt"
)

func main() {
	filename := "test-data/user-1000.journal"
	cursor := "s=69e0bc24292040569344cea3ad97204c;i=810;b=6b84ae3ed1114c0b900c8c464e64a015;m=155e8d7;t=616c4f6c535b6;x=23a3cd7d2742e8c3"
	reader, err := newReader(filename)

	units := []string{
		"session-12.scope",
	}

	filter := Filter{
		Name:    "_SYSTEMD_UNIT",
		Keep:    true,
		Matches: units,
	}

	filterChain := FilterChain{
		OperatorOr:   true,
		FilterChains: []FilterChain{},
		Filters:      []Filter{filter},
	}

	if err != nil {
		panic(err)
	}

	err = reader.goToCursor(cursor)
	if err != nil {
		panic(err)
	}

	go reader.readAll(context.Background())

	for log := range reader.data {
		if !filterChain.filterIn(log.attributes) {
			fmt.Printf("Rejecting \n\n")
			continue
		}
		fmt.Printf("\n\n")
		for key, value := range log.attributes {
			fmt.Printf("%v=%v\n", key, value)
		}
	}
}
