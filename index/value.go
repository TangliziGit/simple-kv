package index

type Value struct {
	Versions *VersionChain
}

func NewValue(val string) *Value {
	chain := NewVersionChain()
	chain.Header = NewVersion(val)
	return &Value{chain}
}
