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

package ec

import (
	"bytes"
	"crypto/rand"
	"math"
	mrand "math/rand"
	"reflect"
	"testing"

	"github.com/cubefs/cubefs/blobstore/common/codemode"
	"github.com/cubefs/cubefs/blobstore/util/bytespool"
	"github.com/cubefs/cubefs/blobstore/util/log"
	"github.com/stretchr/testify/require"
)

var srcData = []byte("Hello world")

func copyShards(a [][]byte) [][]byte {
	b := make([][]byte, len(a))
	for i := range a {
		b[i] = append(b[i], a[i]...)
	}
	return b
}

func TestEncoderNew(t *testing.T) {
	{
		_, err := NewEncoder(Config{CodeMode: codemode.Tactic{}})
		require.ErrorIs(t, err, ErrInvalidCodeMode)
	}
	{
		_, err := NewEncoder(Config{CodeMode: codemode.EC15P12.Tactic()})
		require.NoError(t, err)
		_, err = NewEncoder(Config{CodeMode: codemode.EC16P20L2.Tactic()})
		require.NoError(t, err)
		// test engine of azurelrcP1
		_, err = NewEncoder(Config{CodeMode: codemode.EC12P6L3.Tactic()})
		require.NoError(t, err)
	}
}

func TestEncoder(t *testing.T) {
	cfg := Config{
		CodeMode:     codemode.EC15P12.Tactic(),
		EnableVerify: true,
		Concurrency:  10,
	}
	encoder, err := NewEncoder(cfg)
	require.NoError(t, err)

	// source data split
	shards, err := encoder.Split(srcData)
	require.NoError(t, err)

	// encode data
	err = encoder.Encode(shards)
	require.NoError(t, err)
	wbuff := bytes.NewBuffer(make([]byte, 0))
	err = encoder.Join(wbuff, shards, len(srcData))
	require.NoError(t, err)
	require.Equal(t, srcData, wbuff.Bytes())

	dataShards := encoder.GetDataShards(shards)
	// set one data shards broken
	for i := range dataShards[0] {
		dataShards[0][i] = 222
	}
	// reconstruct data and check
	err = encoder.ReconstructData(shards, []int{0})
	require.NoError(t, err)
	wbuff = bytes.NewBuffer(make([]byte, 0))
	err = encoder.Join(wbuff, shards, len(srcData))
	require.NoError(t, err)
	require.Equal(t, srcData, wbuff.Bytes())

	// reconstruct shard and check
	parityShards := encoder.GetParityShards(shards)
	for i := range parityShards[1] {
		parityShards[1][i] = 11
	}
	err = encoder.Reconstruct(shards, []int{cfg.CodeMode.N + 1})
	require.NoError(t, err)
	ok, err := encoder.Verify(shards)
	require.NoError(t, err)
	require.True(t, ok)
	wbuff = bytes.NewBuffer(make([]byte, 0))
	err = encoder.Join(wbuff, shards, len(srcData))
	require.NoError(t, err)
	require.Equal(t, srcData, wbuff.Bytes())

	ls := encoder.GetLocalShards(shards)
	require.Equal(t, 0, len(ls))
	si := encoder.GetShardsInIdc(shards, 0)
	require.Equal(t, (cfg.CodeMode.N+cfg.CodeMode.M)/3, len(si))
}

func TestLrcEncoder(t *testing.T) {
	cfg := Config{
		CodeMode:     codemode.EC6P10L2.Tactic(),
		EnableVerify: true,
	}
	encoder, err := NewEncoder(cfg)
	require.NoError(t, err)

	_, err = encoder.Split([]byte{})
	require.Error(t, err)

	// source data split
	shards, err := encoder.Split(srcData)
	require.NoError(t, err)
	{
		enoughBuff := make([]byte, 1<<10)
		copy(enoughBuff, srcData)
		enoughBuff = enoughBuff[:len(srcData)]
		_, err := encoder.Split(enoughBuff)
		require.NoError(t, err)
	}

	invalidShards := shards[:len(shards)-1]
	require.ErrorIs(t, encoder.Encode(invalidShards), ErrInvalidShards)
	require.ErrorIs(t, encoder.Encode(nil), ErrInvalidShards)

	// encode data
	err = encoder.Encode(shards)
	require.NoError(t, err)
	wbuff := bytes.NewBuffer(make([]byte, 0))
	err = encoder.Join(wbuff, shards, len(srcData))
	require.NoError(t, err)
	require.Equal(t, srcData, wbuff.Bytes())

	dataShards := encoder.GetDataShards(shards)
	// set one data shard broken
	for i := range dataShards[0] {
		dataShards[0][i] = 222
	}

	// test verify failed
	ok, err := encoder.Verify(shards)
	require.NoError(t, err)
	require.False(t, ok)

	// reconstruct data and check
	err = encoder.ReconstructData(shards, []int{0})
	require.NoError(t, err)
	wbuff = bytes.NewBuffer(make([]byte, 0))
	err = encoder.Join(wbuff, shards, len(srcData))
	require.NoError(t, err)
	require.Equal(t, srcData, wbuff.Bytes())

	// Local reconstruct shard and check
	localShardsInIdc := encoder.GetShardsInIdc(shards, 0)
	for idx := 0; idx < len(localShardsInIdc); idx++ {
		// set wrong data
		for i := range localShardsInIdc[idx] {
			localShardsInIdc[idx][i] = 11
		}
		// check must be false when a shard broken
		ok, err := encoder.Verify(shards)
		require.NoError(t, err)
		require.False(t, ok)

		err = encoder.Reconstruct(localShardsInIdc, []int{idx})
		require.NoError(t, err)
		ok, err = encoder.Verify(shards)
		require.NoError(t, err)
		require.True(t, ok)
	}

	badIdxs := make([]int, 0)

	// add a local broken
	for j := range shards[cfg.CodeMode.N+cfg.CodeMode.M+1] {
		shards[cfg.CodeMode.N+cfg.CodeMode.M+1][j] = 222
	}
	badIdxs = append(badIdxs, cfg.CodeMode.N+cfg.CodeMode.M+1)

	// test local verify failed
	ok, err = encoder.Verify(shards)
	require.NoError(t, err)
	require.False(t, ok)

	// global reconstruct shard and check
	dataShards = encoder.GetDataShards(shards)
	parityShards := encoder.GetParityShards(shards)
	for i := 0; i < cfg.CodeMode.M; i++ {
		if i%2 == 0 {
			badIdxs = append(badIdxs, i)
			// set wrong data
			if i < len(dataShards) {
				for j := range dataShards[i] {
					dataShards[i][j] = 222
				}
			}
		} else {
			badIdxs = append(badIdxs, cfg.CodeMode.N+i)
			// set wrong data
			for j := range parityShards[i] {
				parityShards[i][j] = 222
			}
		}
	}

	// test verify failed
	ok, err = encoder.Verify(shards)
	require.NoError(t, err)
	require.False(t, ok)

	err = encoder.Reconstruct(shards, badIdxs)
	require.NoError(t, err)
	ok, err = encoder.Verify(shards)
	require.NoError(t, err)
	require.True(t, ok)
	wbuff = bytes.NewBuffer(make([]byte, 0))
	err = encoder.Join(wbuff, shards, len(srcData))
	require.NoError(t, err)
	require.Equal(t, srcData, wbuff.Bytes())

	ls := encoder.GetLocalShards(shards)
	require.Equal(t, cfg.CodeMode.L, len(ls))
	si := encoder.GetShardsInIdc(shards, 0)
	require.Equal(t, (cfg.CodeMode.N+cfg.CodeMode.M+cfg.CodeMode.L)/cfg.CodeMode.AZCount, len(si))

	// test data len
	shards[badIdxs[0]] = shards[badIdxs[0]][:0]
	ok, err = encoder.Verify(shards)
	require.Error(t, err)
	require.False(t, ok)

	err = encoder.Reconstruct(shards, badIdxs)
	require.NoError(t, err)

	shards[badIdxs[len(badIdxs)-1]] = shards[len(badIdxs)-1][:0]
	ok, err = encoder.Verify(shards)
	require.Error(t, err)
	require.False(t, ok)
}

func TestAzureLrcP1Encoder(t *testing.T) {
	for _, cm := range codemode.GetAllCodeModes() {
		if cm.Tactic().CodeType == codemode.AzureLrcP1 {
			testAzureLrcP1Encoder(t, cm)
			log.Printf("cm:%v test finish!", cm)
		}
	}
}

func testAzureLrcP1Encoder(t *testing.T, cm codemode.CodeMode) {
	cfg := Config{
		CodeMode:     cm.Tactic(),
		EnableVerify: true,
	}
	encoder, err := NewEncoder(cfg)
	require.NoError(t, err)

	_, err = encoder.Split([]byte{})
	require.Error(t, err)

	// source data split
	data := make([]byte, len(srcData))
	copy(data, srcData)
	shards, err := encoder.Split(data)
	require.NoError(t, err)
	{
		enoughBuff := make([]byte, 1<<10)
		copy(enoughBuff, srcData)
		enoughBuff = enoughBuff[:len(srcData)]
		_, err := encoder.Split(enoughBuff)
		require.NoError(t, err)
	}

	invalidShards := shards[:len(shards)-1]
	require.ErrorIs(t, encoder.Encode(invalidShards), ErrInvalidShards)
	require.ErrorIs(t, encoder.Encode(nil), ErrInvalidShards)

	// encode data
	err = encoder.Encode(shards)
	require.NoError(t, err)
	wbuff := bytes.NewBuffer(make([]byte, 0))
	err = encoder.Join(wbuff, shards, len(srcData))
	require.NoError(t, err)
	require.Equal(t, srcData, wbuff.Bytes())

	dataShards := encoder.GetDataShards(shards)
	// set one data shard broken
	for i := range dataShards[0] {
		dataShards[0][i] = 222
	}

	// test verify failed
	ok, err := encoder.Verify(shards)
	require.Error(t, err)
	require.False(t, ok)

	// reconstruct data and check
	err = encoder.ReconstructData(shards, []int{0})
	require.NoError(t, err)
	wbuff = bytes.NewBuffer(make([]byte, 0))
	err = encoder.Join(wbuff, shards, len(srcData))
	require.NoError(t, err)
	require.Equal(t, srcData, wbuff.Bytes())

	// Local reconstruct shard and check
	for idx := 0; idx < len(shards); idx++ {
		// set wrong data
		for i := range shards[idx] {
			shards[idx][i] = 11
		}
		// check must be false when a shard broken
		ok, err := encoder.Verify(shards)
		require.Error(t, err)
		require.False(t, ok)

		azLayout := cfg.CodeMode.GetECLayoutByAZ()
		failAzIdx := 0
		for i, az := range azLayout {
			for _, nodeIdx := range az {
				if idx == nodeIdx {
					failAzIdx = i
				}
			}
		}
		tmpShards := make([][]byte, len(shards))
		for _, nodeIdx := range azLayout[failAzIdx] {
			if nodeIdx != idx {
				tmpShards[nodeIdx] = make([]byte, len(shards[nodeIdx]))
				copy(tmpShards[nodeIdx], shards[nodeIdx])
			}
		}
		err = encoder.Reconstruct(tmpShards, []int{idx})
		require.NoError(t, err)
		shards[idx] = tmpShards[idx]
		ok, err = encoder.Verify(shards)
		require.NoError(t, err)
		require.True(t, ok)
	}

	badIdxs := make([]int, 0)
	// M failures test
	dataShards = encoder.GetDataShards(shards)
	parityShards := encoder.GetParityShards(shards)
	comb := func(n, k int) int {
		lgmma1, _ := math.Lgamma(float64(n + 1))
		lgmma2, _ := math.Lgamma(float64(k + 1))
		lgmma3, _ := math.Lgamma(float64(n - k + 1))
		return int(math.Round(math.Exp(lgmma1 - lgmma2 - lgmma3)))
	}
	n := cfg.CodeMode.N + cfg.CodeMode.M + cfg.CodeMode.L
	k := cfg.CodeMode.M
	combinationCnt := comb(n, k)
	tmpCombination := make([]int, k)
	for i := range tmpCombination {
		tmpCombination[i] = i
	}
	if combinationCnt > 100000 {
		combinationCnt = 100000
	}
	for i := 0; i < combinationCnt; i++ {
		badIdxs = make([]int, 0)
		// Obtaining corresponding matrix by the combination
		for _, e := range tmpCombination {
			badIdxs = append(badIdxs, e)
			// set wrong data
			if e < len(dataShards) {
				for j := range dataShards[e] {
					dataShards[e][j] = 222
				}
			} else {
				for j := range parityShards[e-len(dataShards)] {
					parityShards[e-len(dataShards)][j] = 222
				}
			}
		}
		// test verify failed
		ok, err = encoder.Verify(shards)
		require.Error(t, err)
		require.False(t, ok)

		err = encoder.Reconstruct(shards, badIdxs)
		require.NoError(t, err)
		ok, err = encoder.Verify(shards)
		require.NoError(t, err)
		require.True(t, ok)
		wbuff = bytes.NewBuffer(make([]byte, 0))
		err = encoder.Join(wbuff, shards, len(srcData))
		require.NoError(t, err)
		require.Equal(t, srcData, wbuff.Bytes())

		if i < combinationCnt-1 {
			j := k - 1
			for j >= 0 && tmpCombination[j] == n-k+j {
				j--
			}
			tmpCombination[j]++
			for j = j + 1; j < k; j++ {
				tmpCombination[j] = tmpCombination[j-1] + 1
			}
		}
	}

	// M+1 failures test
	// find the combination
	n = cfg.CodeMode.N + cfg.CodeMode.M + cfg.CodeMode.L
	k = cfg.CodeMode.M + 1
	combinationCnt = comb(n, k)
	tmpCombination = make([]int, k)
	for i := range tmpCombination {
		tmpCombination[i] = i
	}
	if combinationCnt > 100000 {
		combinationCnt = 100000
	}
	for i := 0; i < combinationCnt; i++ {
		badIdxs = make([]int, 0)
		// Obtaining corresponding matrix by the combination
		for _, e := range tmpCombination {
			badIdxs = append(badIdxs, e)
			// set wrong data
			if e < len(dataShards) {
				for j := range dataShards[e] {
					dataShards[e][j] = 222
				}
			} else {
				for j := range parityShards[e-len(dataShards)] {
					parityShards[e-len(dataShards)][j] = 222
				}
			}
		}
		// test verify failed
		ok, err = encoder.Verify(shards)
		require.Error(t, err)
		require.False(t, ok)

		err = encoder.Reconstruct(shards, badIdxs)
		require.NoError(t, err)
		ok, err = encoder.Verify(shards)
		require.NoError(t, err)
		require.True(t, ok)
		wbuff = bytes.NewBuffer(make([]byte, 0))
		err = encoder.Join(wbuff, shards, len(srcData))
		require.NoError(t, err)
		require.Equal(t, srcData, wbuff.Bytes())

		if i < combinationCnt-1 {
			j := k - 1
			for j >= 0 && tmpCombination[j] == n-k+j {
				j--
			}
			tmpCombination[j]++
			for j = j + 1; j < k; j++ {
				tmpCombination[j] = tmpCombination[j-1] + 1
			}
		}
	}

	ls := encoder.GetLocalShards(shards)
	require.Equal(t, cfg.CodeMode.L, len(ls))
	// check DATA stripes' shards
	sid := encoder.GetShardsInIdc(shards, 0)
	require.Equal(t, cfg.CodeMode.N/(cfg.CodeMode.AZCount-1)+cfg.CodeMode.L/cfg.CodeMode.AZCount, len(sid))
	// check PARITY stripe's shards
	sip := encoder.GetShardsInIdc(shards, cfg.CodeMode.AZCount-1)
	require.Equal(t, cfg.CodeMode.M+cfg.CodeMode.L/cfg.CodeMode.AZCount, len(sip))

	// test data len
	shards[badIdxs[0]] = shards[badIdxs[0]][:0]
	ok, err = encoder.Verify(shards)
	require.Error(t, err)
	require.False(t, ok)

	err = encoder.Reconstruct(shards, badIdxs)
	require.NoError(t, err)

	shards[badIdxs[len(badIdxs)-1]] = shards[badIdxs[len(badIdxs)-1]][:0]
	ok, err = encoder.Verify(shards)
	require.Error(t, err)
	require.False(t, ok)
}

func TestLrcReconstruct(t *testing.T) {
	for _, cm := range codemode.GetAllCodeModes() {
		if cm.Tactic().CodeType == codemode.OPPOLrc {
			testLrcReconstruct(t, cm)
		}
	}
}

func testLrcReconstruct(t *testing.T, cm codemode.CodeMode) {
	tactic := cm.Tactic()
	cfg := Config{CodeMode: tactic, EnableVerify: true}
	encoder, _ := NewEncoder(cfg)

	data := make([]byte, (1<<16)+mrand.Intn(1<<16))
	rand.Read(data)

	shards, err := encoder.Split(data)
	require.NoError(t, err)
	require.NoError(t, encoder.Encode(shards))

	origin := copyShards(shards)
	bads := make([]int, 0)
	for badIdx := tactic.N + tactic.M; badIdx < cm.GetShardNum(); badIdx++ {
		bads = append(bads, badIdx)
		for _, idx := range bads {
			bytespool.Zero(shards[idx])
			shards[idx] = shards[idx][:0]
		}
		require.NoError(t, encoder.Reconstruct(shards, bads))
		require.True(t, reflect.DeepEqual(origin, shards))
	}
	for badIdx := 0; badIdx < tactic.N+tactic.M; badIdx++ {
		bads = append(bads, badIdx)
	}
	require.Error(t, encoder.Reconstruct(copyShards(shards), bads))

	// use local ec reconstruct
	for azIdx := 0; azIdx < tactic.AZCount; azIdx++ {
		locals, n, m := tactic.LocalStripeInAZ(azIdx)
		var localShards [][]byte
		for _, idx := range locals {
			localShards = append(localShards, shards[idx])
		}
		localOrigin := copyShards(localShards)

		bads := make([]int, 0)
		for badIdx := n; badIdx < n+m; badIdx++ {
			bads = append(bads, badIdx)
			for _, idx := range bads {
				bytespool.Zero(localShards[idx])
				localShards[idx] = localShards[idx][:0]
			}
			require.NoError(t, encoder.Reconstruct(localShards, bads))
			require.True(t, reflect.DeepEqual(localOrigin, localShards))
		}
		if n > 0 {
			bads = append(bads, n-1)
			require.Error(t, encoder.Reconstruct(localShards, bads))
		}
	}
}

func TestReedSolomonPartialReconstruct(t *testing.T) {
	for _, cm := range codemode.GetAllCodeModes() {
		if cm.Tactic().CodeType == codemode.ReedSolomon {
			testReedSolomonPartialReconstruct(t, cm)
		}
	}
}

func testReedSolomonPartialReconstruct(t *testing.T, cm codemode.CodeMode) {
	cfg := Config{
		CodeMode:     cm.Tactic(),
		EnableVerify: true,
	}
	log.Println(cfg.CodeMode)
	encoder, err := NewEncoder(cfg)
	require.NoError(t, err)

	_, err = encoder.Split([]byte{})
	require.Error(t, err)

	// source data split
	shards, err := encoder.Split(srcData)
	require.NoError(t, err)
	{
		enoughBuff := make([]byte, 1<<10)
		copy(enoughBuff, srcData)
		enoughBuff = enoughBuff[:len(srcData)]
		_, err := encoder.Split(enoughBuff)
		require.NoError(t, err)
	}

	// encode data
	err = encoder.Encode(shards)
	require.NoError(t, err)
	wbuff := bytes.NewBuffer(make([]byte, 0))
	err = encoder.Join(wbuff, shards, len(srcData))
	require.NoError(t, err)
	require.Equal(t, srcData, wbuff.Bytes())

	// single failure reconstruct
	for idx := 0; idx < len(shards); idx++ {
		// set wrong data
		for i := range shards[idx] {
			shards[idx][i] = 222
		}
		// check must be false when a shard broken
		ok, err := encoder.Verify(shards)
		require.NoError(t, err)
		require.False(t, ok)

		err = encoder.Reconstruct(shards, []int{idx})
		require.NoError(t, err)
		ok, err = encoder.Verify(shards)
		require.NoError(t, err)
		require.True(t, ok)
	}

	// single failure partial reconstruct
	azLayout := cfg.CodeMode.GetECLayoutByAZ()
	for idx := 0; idx < len(shards); idx++ {
		// set wrong data
		for i := range shards[idx] {
			shards[idx][i] = 222
		}
		ok, _ := encoder.Verify(shards)
		require.False(t, ok)

		azCnt := len(azLayout)
		survivalIdxMap := make(map[int]int)
		survivalIdx, _, _ := encoder.GetSurvivalShards([]int{idx}, azLayout)
		survivalCnt := 0
		for _, s := range survivalIdx {
			survivalIdxMap[s] = 1
			survivalCnt++
		}

		partialData := make([]byte, len(shards[0]))
		for c := range partialData {
			partialData[c] = 0
		}
		// start partial reconstruct
		for azIdx := 0; azIdx < azCnt; azIdx++ {
			tmpShards := make([][]byte, cfg.CodeMode.N+cfg.CodeMode.M)
			for _, v := range azLayout[azIdx] {
				if _, ok := survivalIdxMap[v]; ok == true {
					tmpShards[v] = shards[v]
				} else {
					tmpShards[v] = shards[v][:0]
				}
			}
			err = encoder.PartialReconstruct(tmpShards[:cfg.CodeMode.N+cfg.CodeMode.M], survivalIdx, []int{idx})
			require.NoError(t, err)
			for c := range partialData {
				partialData[c] ^= tmpShards[idx][c]
			}
		}
		shards[idx] = partialData

		ok, err = encoder.Verify(shards)
		require.NoError(t, err)
		require.True(t, ok)
	}
}
