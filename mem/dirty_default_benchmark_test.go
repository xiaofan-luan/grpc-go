/*
 *
 * Copyright 2026 gRPC authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package mem

import (
	"fmt"
	"runtime"
	"testing"

	internalmem "google.golang.org/grpc/internal/mem"
)

var compareSizes = []int{1024, 33 << 10, 64 << 10, 1 << 20, 4 << 20, 16 << 20}

func comparePool(b *testing.B, dirty bool) BufferPool {
	b.Helper()
	var p BufferPool
	var err error
	if dirty {
		p, err = internalmem.NewDirtyBinaryTieredBufferPool(defaultBufferPoolSizeExponents...)
	} else {
		p, err = internalmem.NewBinaryTieredBufferPool(defaultBufferPoolSizeExponents...)
	}
	if err != nil {
		b.Fatal(err)
	}
	return p
}
func compareFragments(n int) BufferSlice {
	s := BufferSlice{}
	// At least two buffers, matching materialization rather than the ref-only path.
	frag := 16 << 10
	if n <= frag {
		frag = n / 2
	}
	for left := n; left > 0; {
		k := min(left, frag)
		v := make([]byte, k)
		for i := range v {
			v[i] = byte(i + 1)
		}
		s = append(s, SliceBuffer(v))
		left -= k
	}
	return s
}
func compareWarm(p BufferPool, n int) {
	v := p.Get(n)
	for i := range *v {
		(*v)[i] = 0xa5
	}
	p.Put(v)
}
func BenchmarkSameTier(b *testing.B) {
	for _, op := range []string{"GetPut", "CopyGet", "Materialize"} {
		for _, n := range compareSizes {
			for _, dirty := range []bool{false, true} {
				name := "clear"
				if dirty {
					name = "dirty"
				}
				b.Run(fmt.Sprintf("%s/%d/%s", op, n, name), func(b *testing.B) {
					p := comparePool(b, dirty)
					s := compareFragments(n)
					defer s.Free()
					compareWarm(p, n)
					b.SetBytes(int64(n))
					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						if op == "Materialize" {
							v := s.MaterializeToBuffer(p)
							runtime.KeepAlive(v.ReadOnlyData())
							v.Free()
						} else {
							v := p.Get(n)
							if op == "CopyGet" {
								s.CopyTo(*v)
							}
							runtime.KeepAlive(*v)
							p.Put(v)
						}
					}
				})
			}
		}
	}
}
func BenchmarkSameTierMixedParallel(b *testing.B) {
	for _, dirty := range []bool{false, true} {
		name := "clear"
		if dirty {
			name = "dirty"
		}
		b.Run(name, func(b *testing.B) {
			p := comparePool(b, dirty)
			ss := make([]BufferSlice, len(compareSizes))
			for i, n := range compareSizes {
				ss[i] = compareFragments(n)
				held := make([]*[]byte, 8)
				for k := range held {
					held[k] = p.Get(n)
					for j := range *held[k] {
						(*held[k])[j] = 0xa5
					}
				}
				for _, v := range held {
					p.Put(v)
				}
			}
			defer func() {
				for _, s := range ss {
					s.Free()
				}
			}()
			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				i := 0
				for pb.Next() {
					v := ss[i%len(ss)].MaterializeToBuffer(p)
					runtime.KeepAlive(v.ReadOnlyData())
					v.Free()
					i++
				}
			})
		})
	}
}
