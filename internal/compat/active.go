package compat

type ActiveFeatures struct {
	set map[string]bool
}

func NewActiveFeatures() ActiveFeatures {
	return ActiveFeatures{set: make(map[string]bool)}
}

func (a ActiveFeatures) With(capability string) ActiveFeatures {
	cp := NewActiveFeatures()
	for k, v := range a.set {
		cp.set[k] = v
	}
	cp.set[capability] = true
	return cp
}

func (a ActiveFeatures) Has(capability string) bool {
	return a.set[capability]
}

func (a ActiveFeatures) List() []string {
	out := make([]string, 0, len(a.set))
	for k := range a.set {
		out = append(out, k)
	}
	return out
}
