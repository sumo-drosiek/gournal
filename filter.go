package main

type Filter struct {
	Name    string
	Matches []string
	Keep    bool
}

func (f *Filter) filterIn(attributes map[string]string) bool {
	value, ok := attributes[f.Name]
	if f.Keep && !ok {
		return false
	}

	for _, match := range f.Matches {
		if value == match && f.Keep {
			return true
		}
	}

	return false
}

type FilterChain struct {
	OperatorOr   bool
	FilterChains []FilterChain
	Filters      []Filter
}

func (fc *FilterChain) filterIn(attributes map[string]string) bool {
	switch fc.OperatorOr {
	case true:
		for _, chain := range fc.FilterChains {
			if chain.filterIn(attributes) {
				return true
			}
		}
		for _, filter := range fc.Filters {
			if filter.filterIn(attributes) {
				return true
			}
		}
		return false
	case false:
		for _, chain := range fc.FilterChains {
			if !chain.filterIn(attributes) {
				return false
			}
		}
		for _, filter := range fc.Filters {
			if !filter.filterIn(attributes) {
				return false
			}
		}
		return true
	}
	return false
}
