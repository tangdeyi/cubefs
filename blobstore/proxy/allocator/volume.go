// Copyright 2022 The CubeFS Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package allocator

import (
	"sort"
	"sync"
	"sync/atomic"

	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/common/proto"
)

type volume struct {
	clustermgr.AllocVolumeInfo
	deleted bool
	mu      sync.RWMutex
}

const (
	ALLOCATING = true
	ALLOCATED  = false
)

type volumes struct {
	vols       []*volume
	totalFree  uint64
	allocating bool

	sync.RWMutex
}

func (s *volumes) Get(vid proto.Vid) (*volume, bool) {
	s.RLock()
	defer s.RUnlock()
	i, ok := search(s.vols, vid)
	if !ok {
		return nil, false
	}
	return s.vols[i], true
}

func (s *volumes) UpdateTotalFree(fsize uint64) uint64 {
	return atomic.AddUint64(&s.totalFree, fsize)
}

func (s *volumes) TotalFree() uint64 {
	return atomic.LoadUint64(&s.totalFree)
}

func (s *volumes) Put(vol *volume) {
	s.Lock()
	defer s.Unlock()
	idx, ok := search(s.vols, vol.Vid)
	if !ok {
		atomic.AddUint64(&s.totalFree, vol.Free)
		s.vols = append(s.vols, vol)
		if idx == len(s.vols)-1 {
			return
		}
		copy(s.vols[idx+1:], s.vols[idx:len(s.vols)-1])
		s.vols[idx] = vol
	}
}

func (s *volumes) Delete(vid proto.Vid) bool {
	s.Lock()
	defer s.Unlock()
	i, ok := search(s.vols, vid)
	if ok {
		atomic.AddUint64(&s.totalFree, -s.vols[i].Free)
		copy(s.vols[i:], s.vols[i+1:])
		s.vols[len(s.vols)-1] = nil
		s.vols = s.vols[:len(s.vols)-1]
	}
	return ok
}

func (s *volumes) List() (vols []*volume) {
	s.RLock()
	defer s.RUnlock()
	vols = s.vols[:]
	return vols
}

func (s *volumes) SetAllocateState(state bool) {
	s.Lock()
	s.allocating = state
	s.Unlock()
}

func (s *volumes) IsAllocating() bool {
	s.RLock()
	defer s.RUnlock()
	return s.allocating
}

func (s *volumes) Len() int {
	s.RLock()
	defer s.RUnlock()
	return len(s.vols)
}

func search(vols []*volume, vid proto.Vid) (int, bool) {
	idx := sort.Search(len(vols), func(i int) bool {
		return vols[i].Vid >= vid
	})
	if idx == len(vols) || vols[idx].Vid != vid {
		return idx, false
	}
	return idx, true
}
