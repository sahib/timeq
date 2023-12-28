package vlog

type Iters []Iter

// NOTE: This is more or less a copy of container/heap in Go's standard
// library. It was modified to directly use the Iter struct instead of relying
// on heap.Interface. This allows the compiler to inline a lot more and to skip
// the expensive Len() check on every Fix() call (which is only expensive
// because it's behind an interface).
//
// This seemingly desperate measure gives us about 10% better performance for
// Pop() heavy workloads!

func (is *Iters) Push(i Iter) {
	*is = append(*is, i)
	is.up(is.Len() - 1)
}

func (is *Iters) Len() int {
	return len(*is)
}

func (is *Iters) Fix(i int) {
	if !is.down(i, is.Len()) {
		is.up(i)
	}
}

func (is Iters) swap(i, j int) {
	is[i], is[j] = is[j], is[i]
}

func (is Iters) less(i, j int) bool {
	if is[i].exhausted != is[j].exhausted {
		// sort exhausted iters to the back
		return !is[i].exhausted
	}

	return is[i].item.Key < is[j].item.Key
}

func (is *Iters) up(j int) {
	for {
		i := (j - 1) / 2 // parent
		if i == j || !is.less(j, i) {
			break
		}

		is.swap(i, j)
		j = i
	}
}

func (is *Iters) down(i0, n int) bool {
	i := i0
	for {
		j1 := 2*i + 1
		if j1 >= n || j1 < 0 { // j1 < 0 after int overflow
			break
		}
		j := j1 // left child
		if j2 := j1 + 1; j2 < n && is.less(j2, j1) {
			j = j2 // = 2*i + 2  // right child
		}
		if !is.less(j, i) {
			break
		}
		is.swap(i, j)
		i = j
	}

	return i > i0
}
