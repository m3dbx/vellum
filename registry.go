//  Copyright (c) 2017 Couchbase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// 		http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package vellum

type registryCell struct {
	addr int
	node *builderNode
}

// Registry is used as a form of LRU so that the number of nodes that need to be kept
// in memory is reduced. When the builder is compiling the FST and is presented with
// compiling a given node, it can check the registry to see if an equivalent node has
// already been compiled. If so, the registry will return the address of the already
// compiled node and the builder can use that. If an equivalent node has not already
// been compiled (or was, but has since been evicted from the LRU), the builder will
// recompile it into the encoder and then add it to the registry for future use.
type registry struct {
	builderNodePool *builderNodePool
	table           []registryCell
	tableSize       uint
	mruSize         uint
	dirtyBuckets    []uint16
}

func newRegistry(p *builderNodePool, tableSize, mruSize int) *registry {
	nsize := tableSize * mruSize
	rv := &registry{
		builderNodePool: p,
		table:           make([]registryCell, nsize),
		tableSize:       uint(tableSize),
		mruSize:         uint(mruSize),
	}
	if tableSize < (2 << (16 - 1)) {
		// Can represent this in the dirty struct of uin16 start offset.
		rv.dirtyBuckets = make([]uint16, 0, tableSize)
	}
	return rv
}

func (r *registry) Reset() {
	// Return any dirty buckets to the pool.
	for i := range r.dirtyBuckets {
		start := r.mruSize * uint(r.dirtyBuckets[i])
		end := start + r.mruSize
		for j := start; j < end; j++ {
			if r.table[j].node != nil {
				r.builderNodePool.Put(r.table[j].node)
			}
		}
	}
	// Reset the dirty buckets slice.
	r.dirtyBuckets = r.dirtyBuckets[:0]

	// Use the memclear for loop optimization to clear the registry.
	var empty registryCell
	for i := range r.table {
		r.table[i] = empty
	}
}

func (r *registry) entry(node *builderNode) (bool, int, *registryCell) {
	if len(r.table) == 0 {
		return false, 0, nil
	}
	bucket := r.hash(node)
	start := r.mruSize * uint(bucket)
	end := start + r.mruSize
	if r.dirtyBuckets != nil && r.table[start].node == nil {
		// This bucket hasn't been used yet so we should mark it as dirty now
		// and just the first time.
		r.dirtyBuckets = append(r.dirtyBuckets, uint16(bucket))
	}
	rc := registryCache(r.table[start:end])
	return rc.entry(node, r.builderNodePool)
}

const fnvPrime = 1099511628211

func (r *registry) hash(b *builderNode) int {
	var final uint64
	if b.final {
		final = 1
	}

	var h uint64 = 14695981039346656037
	h = (h ^ final) * fnvPrime
	h = (h ^ b.finalOutput) * fnvPrime
	for _, t := range b.trans {
		h = (h ^ uint64(t.in)) * fnvPrime
		h = (h ^ t.out) * fnvPrime
		h = (h ^ uint64(t.addr)) * fnvPrime
	}
	return int(h % uint64(r.tableSize))
}

type registryCache []registryCell

// The registry is responsible for returning BuilderNodes that it controls to the BuilderNodePool once
// they are evicted. As a result, all the codepaths in the entry method that return false (entry was not
// found and the registry is assuming ownership of this node) will return the corresponding evicted node to
// the builderNodePool.
func (r registryCache) entry(node *builderNode, pool *builderNodePool) (bool, int, *registryCell) {
	if len(r) == 1 {
		if r[0].node != nil && r[0].node.equiv(node) {
			return true, r[0].addr, nil
		}
		pool.Put(r[0].node)
		r[0].node = node
		return false, 0, &r[0]
	}
	for i := range r {
		if r[i].node != nil && r[i].node.equiv(node) {
			addr := r[i].addr
			r.promote(i)
			return true, addr, nil
		}
	}

	// no match
	last := len(r) - 1
	pool.Put(r[last].node)
	r[last].node = node // discard LRU
	r.promote(last)
	return false, 0, &r[0]
}

func (r registryCache) promote(i int) {
	for i > 0 {
		r.swap(i-1, i)
		i--
	}
}

func (r registryCache) swap(i, j int) {
	r[i], r[j] = r[j], r[i]
}
