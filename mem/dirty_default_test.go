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

package mem_test

import (
	"bytes"
	"errors"
	"io"
	"testing"
	"unsafe"

	"google.golang.org/grpc/mem"
)

func TestDefaultBufferPoolDoesNotClearReusedBuffer(t *testing.T) {
	pool := mem.DefaultBufferPool()
	for attempt := 0; attempt < 100; attempt++ {
		first := pool.Get(32 << 10)
		for i := range *first {
			(*first)[i] = 0xdd
		}
		pointer := unsafe.SliceData(*first)
		pool.Put(first)
		second := pool.Get(32 << 10)
		same := unsafe.SliceData(*second) == pointer
		dirty := bytes.Equal(*second, bytes.Repeat([]byte{0xdd}, 32<<10))
		pool.Put(second)
		if same {
			if !dirty {
				t.Fatal("default pool cleared reused buffer")
			}
			return
		}
	}
	t.Skip("pool did not reuse a buffer; sync.Pool reuse is not guaranteed")
}

type dirtyDefaultPool struct{ mem.BufferPool }

func (p dirtyDefaultPool) Get(n int) *[]byte {
	b := p.BufferPool.Get(n)
	for i := range *b {
		(*b)[i] = 0xdd
	}
	return b
}

type dirtyPartialReader struct{ terminal error }

func (r dirtyPartialReader) Read(p []byte) (int, error) { return copy(p, []byte("safe")), r.terminal }

func TestReadAllDirtyDefaultPartialRead(t *testing.T) {
	// Poison each acquired buffer deterministically; reuse/GC timing must not
	// decide whether the unwritten tail contains stale bytes during this test.
	pool := dirtyDefaultPool{mem.DefaultBufferPool()}
	for _, terminal := range []error{io.EOF, errors.New("partial read failure")} {
		out, err := mem.ReadAll(dirtyPartialReader{terminal}, pool)
		if terminal == io.EOF && err != nil {
			t.Fatal(err)
		}
		if terminal != io.EOF && !errors.Is(err, terminal) {
			t.Fatalf("wrong error %v", err)
		}
		if out.Len() != 4 || !bytes.Equal(out.Materialize(), []byte("safe")) {
			out.Free()
			t.Fatal("stale tail published")
		}
		out.Free()
	}
}
