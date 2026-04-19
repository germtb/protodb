package protodb

type memtable struct {
	list *Skiplist
}

func newMemtable() *memtable {
	return &memtable{
		list: NewSkiplist(),
	}
}

func (m *memtable) Put(key Key, value []byte, seqnum uint32) {
	m.list.Put(key, value, seqnum)
}

func (m *memtable) Delete(key Key, seqnum uint32) {
	m.list.Delete(key, seqnum)
}

func (m *memtable) Len() int {
	return m.list.Len()
}

func (m *memtable) Get(key Key, snapshotSeq uint32) ([]byte, error) {
	return m.list.Get(key, snapshotSeq)
}

func (m *memtable) ByteSize() uint64 {
	return m.list.ByteSize()
}

func (m *memtable) Scan(lo, hi Key, snapshotSeq uint32) *skiplistIterator {
	return m.list.Scan(lo, hi, snapshotSeq)
}

func (m *memtable) Entries() *skiplistIterator {
	return m.list.Entries()
}
