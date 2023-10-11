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

package volumemgr

import (
	"context"
	"encoding/json"

	"github.com/google/uuid"

	"github.com/cubefs/cubefs/blobstore/api/blobnode"
	cmapi "github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/clustermgr/base"
	"github.com/cubefs/cubefs/blobstore/clustermgr/diskmgr"
	apierrors "github.com/cubefs/cubefs/blobstore/common/errors"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/util/errors"
)

// ListVolumeUnitInfo return disk's volume unit infos, it use index-table disk-vuid as index
// this API is lightly operation, it only call when broken disk or some else, so here is just get data from db
func (v *VolumeMgr) ListVolumeUnitInfo(ctx context.Context, args *cmapi.ListVolumeUnitArgs) ([]*cmapi.VolumeUnitInfo, error) {
	unitPrefixs, err := v.volumeTbl.ListVolumeUnit(args.DiskID)
	if err != nil {
		return nil, errors.Info(err, "head volume unit from tbl failed").Detail(err)
	}

	ret := make([]*cmapi.VolumeUnitInfo, 0)
	for _, unitPrefix := range unitPrefixs {
		record, err := v.volumeTbl.GetVolumeUnit(unitPrefix)
		if err != nil {
			return nil, errors.Info(err, "get volume unit from tbl failed").Detail(err)
		}
		ret = append(ret, volumeUnitRecordToVolumeUnit(record).vuInfo)
	}
	return ret, nil
}

func (v *VolumeMgr) AllocVolumeUnit(ctx context.Context, vuid proto.Vuid) (*cmapi.AllocVolumeUnit, error) {
	span := trace.SpanFromContextSafe(ctx)
	// 校验vid对用的volume是否存在
	vid := vuid.Vid()
	vol := v.all.getVol(vid)
	if vol == nil {
		return nil, ErrVolumeNotExist
	}

	// 校验index是否越界
	index := vuid.Index()
	vol.lock.RLock()
	if index >= uint8(len(vol.vUnits)) {
		vol.lock.RUnlock()
		return nil, ErrVolumeUnitNotExist
	}
	// nextEpoch可以理解为一种中间状态，卷单元vuid已经分配(走AllocVolumeUnit接口)出的最新版本(但可能当前正在使用的不是这个版本)
	nextEpoch := vol.vUnits[index].nextEpoch + 1
	vol.lock.RUnlock()

	pendingVuidKey := uuid.New().String()
	v.pendingEntries.Store(pendingVuidKey, proto.Vuid(0))
	// clear pending entry key
	defer v.pendingEntries.Delete(pendingVuidKey)

	data, err := json.Marshal(&allocVolumeUnitCtx{Vuid: vuid, NextEpoch: nextEpoch, PendingVuidKey: pendingVuidKey})
	if err != nil {
		return nil, errors.Info(err, "json marshal failed").Detail(err)
	}

	err = v.raftServer.Propose(ctx, base.EncodeProposeInfo(v.GetModuleName(), OperTypeAllocVolumeUnit, data, base.ProposeContext{ReqID: span.TraceID()}))
	if err != nil {
		return nil, errors.Info(err, "propose failed").Detail(err)
	}
	newVuid, _ := v.pendingEntries.Load(pendingVuidKey)
	if newVuid.(proto.Vuid) == 0 {
		return nil, apierrors.ErrConcurrentAllocVolumeUnit
	}

	excludes := make([]proto.DiskID, 0)
	targetDiskID := proto.DiskID(0)
	vol.lock.RLock()
	targetDiskID = vol.vUnits[vuid.Index()].vuInfo.DiskID
	// 排除掉vid中其他vuid所属的diskID（disk隔离）
	for _, vu := range vol.vUnits {
		excludes = append(excludes, vu.vuInfo.DiskID)
	}
	vol.lock.RUnlock()

	diskInfo, err := v.diskMgr.GetDiskInfo(ctx, targetDiskID)
	if err != nil {
		return nil, errors.Info(err, "get disk info failed").Detail(err)
	}

	// idc和原来的diskID保持一致
	policy := &diskmgr.AllocPolicy{Idc: diskInfo.Idc, Vuids: []proto.Vuid{newVuid.(proto.Vuid)}, Excludes: excludes}
	allocDiskID, err := v.diskMgr.AllocChunks(ctx, policy)
	if err != nil {
		return nil, errors.Info(err, "alloc chunk failed").Detail(err)
	}

	return &cmapi.AllocVolumeUnit{DiskID: allocDiskID[0], Vuid: newVuid.(proto.Vuid)}, nil
}

func (v *VolumeMgr) PreUpdateVolumeUnit(ctx context.Context, args *cmapi.UpdateVolumeArgs) (err error) {
	span := trace.SpanFromContextSafe(ctx)
	// 校验vid对应的volume是否存在
	vol := v.all.getVol(args.OldVuid.Vid())
	if vol == nil {
		return ErrVolumeNotExist
	}

	vol.lock.RLock()
	defer vol.lock.RUnlock()

	unit := vol.vUnits[args.OldVuid.Index()]
	// 校验OldVuid.Epoch是否旧于unit.nextEpoch ||  OldVuid和NewVuid同时不匹配epoch
	if (proto.EncodeVuid(unit.vuidPrefix, unit.epoch) != args.OldVuid &&
		proto.EncodeVuid(unit.vuidPrefix, unit.epoch) != args.NewVuid) ||
		unit.nextEpoch < args.OldVuid.Epoch() {
		span.Errorf("volume's vuid is %v", proto.EncodeVuid(unit.vuidPrefix, unit.epoch))
		return ErrOldVuidNotMatch
	}
	// 校验NewVuid是否匹配nextEpoch
	if proto.EncodeVuid(unit.vuidPrefix, unit.nextEpoch) != args.NewVuid {
		span.Errorf("volume's vuid is %v", proto.EncodeVuid(unit.vuidPrefix, unit.nextEpoch))
		return ErrNewVuidNotMatch
	}
	// idempotent retry update volume unit, return success, and do not stat chunk from data node
	// 请求超时重试幂等，当前的epoch已经等于NewVuid
	if proto.EncodeVuid(unit.vuidPrefix, unit.epoch) == args.NewVuid {
		return ErrRepeatUpdateUnit
	}

	// 校验diskId是否匹配
	diskInfo, err := v.diskMgr.GetDiskInfo(ctx, args.NewDiskID)
	if err != nil {
		span.Errorf("new diskID:%v not exist", args.NewDiskID)
		return apierrors.ErrCMDiskNotFound
	}
	chunkInfo, err := v.blobNodeClient.StatChunk(ctx, diskInfo.Host, &blobnode.StatChunkArgs{DiskID: args.NewDiskID, Vuid: args.NewVuid})
	if err != nil {
		span.Errorf("stat blob node chunk, disk id[%d], vuid[%d] failed: %s", args.NewDiskID, args.NewVuid, err.Error())
		return apierrors.ErrStatChunkFailed
	}
	if chunkInfo == nil || chunkInfo.DiskID != args.NewDiskID {
		span.Errorf("new diskID:%v not match", args.NewDiskID)
		return ErrNewDiskIDNotMatch
	}

	return nil
}

// ReleaseVolumeUnit release old volumeUnit's old chunk
func (v *VolumeMgr) ReleaseVolumeUnit(ctx context.Context, vuid proto.Vuid, diskID proto.DiskID, force bool) (err error) {
	// 根据diskId查询diskInfo，主要是为了拿到blobnode的host
	diskInfo, err := v.diskMgr.GetDiskInfo(ctx, diskID)
	if err != nil {
		return errors.Info(err, "get disk info failed").Detail(err)
	}
	ReleaseChunkArgs := &blobnode.ChangeChunkStatusArgs{
		DiskID: diskID,
		Vuid:   vuid,
		Force:  force, // 是否强制release
	}
	// 调用blobnode释放该chunk
	err = v.blobNodeClient.ReleaseChunk(ctx, diskInfo.Host, ReleaseChunkArgs)

	return
}

func (v *VolumeMgr) applyUpdateVolumeUnit(ctx context.Context, newVuid proto.Vuid, newDiskID proto.DiskID) (err error) {
	span := trace.SpanFromContextSafe(ctx)
	span.Debugf("start apply update volume unit, newVuid is %d, newDiskID is %d", newVuid, newDiskID)

	vol := v.all.getVol(newVuid.Vid())
	if vol == nil {
		span.Errorf("vid:%d get volume is nil ", newVuid.Vid())
		return ErrVolumeNotExist
	}
	index := newVuid.Index()
	if int(index) > len(vol.vUnits) {
		return ErrNewVuidNotMatch
	}

	vol.lock.Lock()

	// 幂等
	if vol.vUnits[index].vuInfo.Vuid == newVuid {
		vol.lock.Unlock()
		return nil
	}

	// when apply wal log happened, the next epoch of volume unit in db may larger than args new vuid's epoch
	// just return nil in this situation
	// 重放wal日志可能会导致vUnit.nextEpoch大于newVuid.Epoch，即vuid持久化的nextEpoch比wal日志的Epoch新
	if vol.vUnits[index].nextEpoch > newVuid.Epoch() {
		span.Debugf("vol nextEpoch: %d bigger than newVuid Epoch : %d", vol.vUnits[index].nextEpoch, newVuid.Epoch())
		vol.lock.Unlock()
		return nil
	}
	diskInfo, err := v.diskMgr.GetDiskInfo(ctx, newDiskID)
	if err != nil {
		span.Errorf("get diskInfo failed,diskID is %d", newDiskID)
		vol.lock.Unlock()
		return err
	}

	// 更新内存中的vuid信息
	vol.vUnits[index].epoch = newVuid.Epoch()
	vol.vUnits[index].vuInfo.DiskID = newDiskID
	vol.vUnits[index].vuInfo.Host = diskInfo.Host
	vol.vUnits[index].vuInfo.Compacting = false
	vol.vUnits[index].vuInfo.Vuid = newVuid

	// 持久化到volumeTbl
	unitRecord := vol.vUnits[index].ToVolumeUnitRecord()
	err = v.volumeTbl.UpdateVolumeUnit(unitRecord.VuidPrefix, unitRecord)
	if err != nil {
		vol.lock.Unlock()
		return err
	}
	vol.lock.Unlock()

	// refresh health
	// vuid对应的chunk更新，刷新volume的健康度
	err = v.refreshHealth(ctx, vol.vid)
	if err != nil {
		span.Errorf("refresh health failed,vid is %d, error is %v", vol.vid, err)
		return err
	}

	span.Debugf("finish apply update volume unit")

	return
}

func (v *VolumeMgr) applyAllocVolumeUnit(ctx context.Context, args *allocVolumeUnitCtx) (err error) {
	span := trace.SpanFromContextSafe(ctx)
	span.Debugf("start apply alloc volume unit, args is %v", args)

	vol := v.all.getVol(args.Vuid.Vid())
	if vol == nil {
		span.Errorf("vid:%d get volume is nil ", args.Vuid.Vid())
		return ErrVolumeNotExist
	}

	idx := args.Vuid.Index()
	vol.lock.Lock()
	// concurrent alloc volume unit or wal log replay, do nothing and return
	if vol.vUnits[idx].nextEpoch >= args.NextEpoch { // 当前vuid的nextEpoch已经大于请求的nextEpoch
		vol.lock.Unlock()
		return
	}
	vol.vUnits[idx].nextEpoch = args.NextEpoch // 更新nextEpoch
	vuRecord := vol.vUnits[idx].ToVolumeUnitRecord()
	err = v.volumeTbl.PutVolumeUnit(args.Vuid.VuidPrefix(), vuRecord) // 持久化到volumeTbl
	if err != nil {
		vol.lock.Unlock()
		return err
	}
	newVuid := proto.EncodeVuid(args.Vuid.VuidPrefix(), vol.vUnits[idx].nextEpoch)
	vol.lock.Unlock()

	// set pending entry in current process context
	if _, ok := v.pendingEntries.Load(args.PendingVuidKey); ok {
		span.Debugf("new vuid is %d", newVuid)
		v.pendingEntries.Store(args.PendingVuidKey, newVuid)
	}

	span.Debugf("finish apply alloc volume unit")

	return
}

// applyChunkReport only change volume unit space info : free/used/total
func (v *VolumeMgr) applyChunkReport(ctx context.Context, chunks *cmapi.ReportChunkArgs) (err error) {
	span := trace.SpanFromContextSafe(ctx)

	for _, chunk := range chunks.ChunkInfos {
		// 根据vuid拿到vid，再获取到volume信息
		vol := v.all.getVol(chunk.Vuid.Vid())
		if vol == nil {
			span.Warnf("vid not found, vid: %d, vuid: %d", chunk.Vuid.Vid(), chunk.Vuid)
			continue
		}
		// 根据vuid拿到当前chunk的index
		idx := chunk.Vuid.Index()
		vol.lock.Lock()
		// in some case, the report vuid epoch may not equal epoch in cm, like balance, we should just ignore it and do not modify
		// todo 为何balance期间blobnode上报的epoch不等于cm存储的，可能要看scheduler的逻辑
		if vol.vUnits[idx].vuInfo.Vuid != chunk.Vuid {
			vol.lock.Unlock()
			continue
		}
		// 修改vuid的free、used、total信息
		vol.vUnits[idx].vuInfo.Free = chunk.Free
		vol.vUnits[idx].vuInfo.Used = chunk.Used
		vol.vUnits[idx].vuInfo.Total = chunk.Total

		// 数据块个数
		dataChunkNum := uint64(v.codeMode[vol.volInfoBase.CodeMode].tactic.N)
		volFree := vol.vUnits[idx].vuInfo.Free * dataChunkNum
		volUsed := vol.vUnits[idx].vuInfo.Used * dataChunkNum
		volTotal := vol.vUnits[idx].vuInfo.Total * dataChunkNum

		// use the minimum free size as volume free
		// vuids的地位是平等的，但可能会出现不同的vuid有着不同的Free空间，所以取Free空间最小的vuid作为卷的Free空间
		if vol.volInfoBase.Free > volFree {
			vol.volInfoBase.Used = volUsed
			vol.volInfoBase.Total = volTotal
			vol.smallestVUIdx = idx
			vol.setFree(ctx, volFree) // freeSize变化会触发volAllocator.VolumeFreeHealthCallback回调，最终可能会添加到可分配卷列表中
		} else {
			// ensure volume free size and use size can be update after shard delete or compaction
			vol.volInfoBase.Used = vol.vUnits[vol.smallestVUIdx].vuInfo.Used * dataChunkNum
			vol.volInfoBase.Total = vol.vUnits[vol.smallestVUIdx].vuInfo.Total * dataChunkNum
			vol.setFree(ctx, vol.vUnits[vol.smallestVUIdx].vuInfo.Free*dataChunkNum)
		}
		vol.lock.Unlock()

		// put on dirty volumes and flush asynchronously
		dirty := v.dirty.Load().(*shardedVolumes)
		dirty.putVol(vol)
	}
	return
}

func (v *VolumeMgr) applyChunkSetCompact(ctx context.Context, args *cmapi.SetCompactChunkArgs) (err error) {
	span := trace.SpanFromContextSafe(ctx)
	span.Debugf("start apply chunk set compact")

	vol := v.all.getVol(args.Vuid.Vid())
	if vol == nil {
		span.Errorf("vid:%d get volume is nil ", args.Vuid.Vid())
		return ErrVolumeNotExist
	}

	index := args.Vuid.Index()

	vol.lock.Lock()
	if index >= uint8(len(vol.vUnits)) || vol.vUnits[index].vuidPrefix != args.Vuid.VuidPrefix() {
		vol.lock.Unlock()
		return ErrVolumeUnitNotExist
	}
	if vol.vUnits[args.Vuid.Index()].vuInfo.Compacting == args.Compacting {
		vol.lock.Unlock()
		return nil
	}

	vol.vUnits[args.Vuid.Index()].vuInfo.Compacting = args.Compacting

	err = v.volumeTbl.PutVolumeUnit(args.Vuid.VuidPrefix(), vol.vUnits[args.Vuid.Index()].ToVolumeUnitRecord())
	vol.lock.Unlock()
	if err != nil {
		return errors.Info(err, "put volume unit failed").Detail(err)
	}

	// refresh volume health
	err = v.refreshHealth(ctx, args.Vuid.Vid())
	if err != nil {
		span.Errorf("refresh health failed,vid is %d", vol.vid)
		return errors.Info(err, "refresh volume health failed").Detail(err)
	}

	span.Debugf("finish apply chunk set compact")

	return
}

// applyDiskWritableChange apply disk's volume refresh
func (v *VolumeMgr) applyDiskWritableChange(ctx context.Context, vuidPrefixes []proto.VuidPrefix) (err error) {
	span := trace.SpanFromContextSafe(ctx)
	span.Debugf("refresh vuid list: %v", vuidPrefixes)
	for _, vuidPrefix := range vuidPrefixes {
		vid := vuidPrefix.Vid()
		// 刷新每个volume的健康度
		err = v.refreshHealth(ctx, vid)
		if err != nil {
			return
		}
	}
	return
}
