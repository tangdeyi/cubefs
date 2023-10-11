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

package clustermgr

import (
	"encoding/json"

	"github.com/cubefs/cubefs/blobstore/api/blobnode"
	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/clustermgr/base"
	"github.com/cubefs/cubefs/blobstore/clustermgr/diskmgr"
	apierrors "github.com/cubefs/cubefs/blobstore/common/errors"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/rpc"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/util/errors"
)

// DiskIdAlloc blobnode磁盘注册DiskAdd前需要调用该接口给磁盘分配diskId
func (s *Service) DiskIdAlloc(c *rpc.Context) {
	ctx := c.Request.Context()
	span := trace.SpanFromContextSafe(ctx)

	span.Info("accept DiskIdAlloc request")
	diskID, err := s.DiskMgr.AllocDiskID(ctx)
	if err != nil {
		span.Errorf("alloc disk id failed =>", errors.Detail(err))
		c.RespondError(err)
		return
	}
	c.RespondJSON(&clustermgr.DiskIDAllocRet{DiskID: diskID})
}

// DiskAdd blobnode先调用DiskIdAlloc申请diskId，再将磁盘信息包括diskId发送注册请求DiskAdd
func (s *Service) DiskAdd(c *rpc.Context) {
	ctx := c.Request.Context()
	span := trace.SpanFromContextSafe(ctx)
	// 请求参数为diskInfo
	args := new(blobnode.DiskInfo)
	if err := c.ParseArgs(args); err != nil {
		c.RespondError(err)
		return
	}
	span.Infof("accept DiskAdd request, args: %v", args)

	info, err := s.DiskMgr.GetDiskInfo(ctx, args.DiskID)
	// 判断diskId是否为重复注册
	if info != nil && err == nil {
		span.Warnf("disk already exist, no need to create again, disk info: %v", args)
		c.RespondError(apierrors.ErrExist)
		return
	}
	// 判断host+path是否为重复注册
	if s.DiskMgr.CheckDiskInfoDuplicated(ctx, args) {
		span.Warnf("disk host and path duplicated")
		c.RespondError(apierrors.ErrIllegalArguments)
		return
	}
	// 判断集群id是否匹配
	if args.ClusterID != s.ClusterID {
		span.Warnf("invalid clusterID")
		c.RespondError(apierrors.ErrIllegalArguments)
		return
	}
	for i := range s.IDC {
		if args.Idc == s.IDC[i] {
			break
		}
		// 判断idc是否找到
		if i == len(s.IDC)-1 {
			span.Warnf("invalid idc %s, service idc: %v", args.Idc, s.IDC)
			c.RespondError(apierrors.ErrIllegalArguments)
			return
		}
	}
	// 校验注册的diskId，其不可能比当前ScopeMgr中最新的diskId还大
	current := s.ScopeMgr.GetCurrent(diskmgr.DiskIDScopeName)
	if proto.DiskID(current) < args.DiskID {
		span.Warnf("invalid disk_id")
		c.RespondError(apierrors.ErrIllegalArguments)
		return
	}

	data, err := json.Marshal(args)
	if err != nil {
		span.Errorf("json marshal failed, disk info: %v, error: %v", args, err)
		c.RespondError(errors.Info(apierrors.ErrUnexpected).Detail(err))
		return
	}
	proposeInfo := base.EncodeProposeInfo(s.DiskMgr.GetModuleName(), diskmgr.OperTypeAddDisk, data, base.ProposeContext{ReqID: span.TraceID()})
	err = s.raftNode.Propose(ctx, proposeInfo)
	if err != nil {
		span.Error("raft propose failed, err: ", err)
		c.RespondError(apierrors.ErrRaftPropose)
	}
}

func (s *Service) DiskInfo(c *rpc.Context) {
	ctx := c.Request.Context()
	span := trace.SpanFromContextSafe(ctx)
	args := new(clustermgr.DiskInfoArgs)
	if err := c.ParseArgs(args); err != nil {
		c.RespondError(err)
		return
	}
	span.Infof("accept DiskInfo request, args: %v", args)

	// linear read
	if err := s.raftNode.ReadIndex(ctx); err != nil {
		span.Errorf("read index error: %v", err)
		c.RespondError(apierrors.ErrRaftReadIndex)
		return
	}

	ret, err := s.DiskMgr.GetDiskInfo(ctx, args.DiskID)
	if err != nil || ret == nil {
		span.Warnf("disk not found: %d", args.DiskID)
		c.RespondError(err)
		return
	}
	c.RespondJSON(ret)
}

func (s *Service) DiskList(c *rpc.Context) {
	ctx := c.Request.Context()
	span := trace.SpanFromContextSafe(ctx)
	args := new(clustermgr.ListOptionArgs)
	if err := c.ParseArgs(args); err != nil {
		c.RespondError(err)
		return
	}
	span.Infof("accept DiskList request, args: %v", args)

	if err := s.raftNode.ReadIndex(ctx); err != nil {
		span.Errorf("read index error: %v", err)
		c.RespondError(apierrors.ErrRaftReadIndex)
		return
	}

	// idc can not be nil when rack param set
	if args.Rack != "" && args.Idc == "" {
		span.Warnf("can not list disk by rack only")
		c.RespondError(apierrors.ErrIllegalArguments)
		return
	}
	if args.Marker != proto.InvalidDiskID {
		if _, err := s.DiskMgr.GetDiskInfo(ctx, args.Marker); err != nil {
			span.Warnf("invalid marker, marker disk not exist")
			err = apierrors.ErrIllegalArguments
			c.RespondError(err)
			return
		}
	}
	if args.Count == 0 {
		args.Count = 10
	}

	ret, err := s.DiskMgr.ListDiskInfo(ctx, args)
	if err != nil {
		span.Errorf("list disk info failed =>", errors.Detail(err))
		err = errors.Info(apierrors.ErrUnexpected).Detail(err)
		c.RespondError(err)
		return
	}
	c.RespondJSON(ret)
}

// DiskSet 设置磁盘状态，此接口只允许设置diskStatus为normal/broken/repairing/repaired，不允许设置dropped状态
func (s *Service) DiskSet(c *rpc.Context) {
	ctx := c.Request.Context()
	span := trace.SpanFromContextSafe(ctx)
	// 参数解析，包括diskId和磁盘状态diskStatus
	args := new(clustermgr.DiskSetArgs)
	if err := c.ParseArgs(args); err != nil {
		c.RespondError(err)
		return
	}
	span.Infof("accept DiskSet request, args: %v", args)

	// not allow to set disk dropped in this API
	// 这个api只允许设置磁盘状态diskStatus为normal/broken/repairing/repaired，不允许设置dropped状态
	if args.Status < proto.DiskStatusNormal || args.Status >= proto.DiskStatusDropped {
		c.RespondError(apierrors.ErrInvalidStatus)
		return
	}

	// 判断当前diskId是否正在下线dropping，正在下线的disk不允许设置status
	isDropping, err := s.DiskMgr.IsDroppingDisk(ctx, args.DiskID)
	if err != nil {
		c.RespondError(err)
		return
	}
	if isDropping {
		c.RespondError(apierrors.ErrDiskIsDropping)
		return
	}

	diskInfo, err := s.DiskMgr.GetDiskInfo(ctx, args.DiskID)
	if err != nil {
		c.RespondError(err)
		return
	}
	// 幂等性校验
	if diskInfo.Status == args.Status {
		return
	}

	// 校验设置磁盘状态的参数
	err = s.DiskMgr.SetStatus(ctx, args.DiskID, args.Status, false)
	if err != nil {
		span.Errorf("disk set failed =>", errors.Detail(err))
		c.RespondError(err)
		return
	}

	data, err := json.Marshal(args)
	if err != nil {
		span.Errorf("json marshal failed, args: %v, error: %v", args, err)
		c.RespondError(errors.Info(apierrors.ErrUnexpected).Detail(err))
		return
	}
	proposeInfo := base.EncodeProposeInfo(s.DiskMgr.GetModuleName(), diskmgr.OperTypeSetDiskStatus, data, base.ProposeContext{ReqID: span.TraceID()})
	err = s.raftNode.Propose(ctx, proposeInfo)
	if err != nil {
		span.Error("raft propose failed, err: ", err)
		c.RespondError(apierrors.ErrRaftPropose)
		return
	}

	// adjust volume health when setting disk broken
	// 设置坏盘时需要降低其上卷volume的健康度，从而proxy在续租时若volume不满足健康度则不续租
	if args.Status == proto.DiskStatusBroken {
		err = s.VolumeMgr.DiskWritableChange(ctx, args.DiskID)
		c.RespondError(err)
	}
}

// DiskDrop 触发磁盘下线，管控面调用该接口下线磁盘，scheduler会定时拉取待下线列表做数据迁移，迁移完成的磁盘再走 DiskDropped
func (s *Service) DiskDrop(c *rpc.Context) {
	ctx := c.Request.Context()
	span := trace.SpanFromContextSafe(ctx)
	args := new(clustermgr.DiskInfoArgs)
	if err := c.ParseArgs(args); err != nil {
		c.RespondError(err)
		return
	}
	span.Infof("accept DiskDrop request, args: %v", args)

	isDropping, err := s.DiskMgr.IsDroppingDisk(ctx, args.DiskID)
	if err != nil {
		c.RespondError(err)
		return
	}
	// is dropping, then return success
	if isDropping {
		return
	}
	diskInfo, err := s.DiskMgr.GetDiskInfo(ctx, args.DiskID)
	if err != nil {
		c.RespondError(err)
		return
	}
	// only normal disk and readonly can add into dropping list
	// 前置条件校验：只有normal状态且切只读的磁盘才能走磁盘下线，这样做能够和scheduler的后台任务隔离
	if diskInfo.Status != proto.DiskStatusNormal || !diskInfo.Readonly {
		c.RespondError(apierrors.ErrDiskAbnormalOrNotReadOnly)
		return
	}

	data, err := json.Marshal(args)
	if err != nil {
		span.Errorf("WsprpcDiskDrop json marshal failed, args: %v, error: %v", args, err)
		c.RespondError(errors.Info(apierrors.ErrUnexpected).Detail(err))
		return
	}
	proposeInfo := base.EncodeProposeInfo(s.DiskMgr.GetModuleName(), diskmgr.OperTypeDroppingDisk, data, base.ProposeContext{ReqID: span.TraceID()})
	err = s.raftNode.Propose(ctx, proposeInfo)
	if err != nil {
		span.Error("raft propose failed, err: ", err)
		c.RespondError(apierrors.ErrRaftPropose)
	}
}

// DiskDropped scheduler完成待下线磁盘的数据迁移后会调用该接口
func (s *Service) DiskDropped(c *rpc.Context) {
	ctx := c.Request.Context()
	span := trace.SpanFromContextSafe(ctx)
	args := new(clustermgr.DiskInfoArgs)
	if err := c.ParseArgs(args); err != nil {
		c.RespondError(err)
		return
	}
	span.Infof("accept DiskDropped request, args: %v", args)

	diskInfo, err := s.DiskMgr.GetDiskInfo(ctx, args.DiskID)
	if err != nil {
		c.RespondError(err)
		return
	}
	// 幂等性
	if diskInfo.Status == proto.DiskStatusDropped {
		return
	}

	// 1. check disk if dropping
	// dropped的前置条件是dropping
	isDropping, err := s.DiskMgr.IsDroppingDisk(ctx, args.DiskID)
	if err != nil {
		c.RespondError(err)
		return
	}
	// disk is not dropping, then return error
	if !isDropping {
		span.Warnf("disk: %d is not in dropping list", args.DiskID)
		c.RespondError(apierrors.ErrChangeDiskStatusNotAllow)
		return
	}

	// 2. check if disk's chunk has been remove
	// 根据diskId拿到其上的vuids列表，scheduler正常走完数据迁移流程，这上面的数据都应该为空
	volumeUnits, err := s.VolumeMgr.ListVolumeUnitInfo(ctx, &clustermgr.ListVolumeUnitArgs{DiskID: args.DiskID})
	if err != nil {
		c.RespondError(err)
		return
	}
	// vuid不为空，不能够下线该diskId
	if len(volumeUnits) != 0 {
		span.Warnf("disk: %d still has existing volume unit, %v", args.DiskID, volumeUnits)
		c.RespondError(apierrors.ErrDroppedDiskHasVolumeUnit)
		return
	}

	// 3. data propose
	data, err := json.Marshal(args)
	if err != nil {
		span.Errorf("json marshal failed, args: %v, error: %v", args, err)
		c.RespondError(errors.Info(apierrors.ErrUnexpected).Detail(err))
		return
	}
	proposeInfo := base.EncodeProposeInfo(s.DiskMgr.GetModuleName(), diskmgr.OperTypeDroppedDisk, data, base.ProposeContext{ReqID: span.TraceID()})
	err = s.raftNode.Propose(ctx, proposeInfo)
	if err != nil {
		span.Error("raft propose failed, err: ", err)
		c.RespondError(apierrors.ErrRaftPropose)
	}
}

func (s *Service) DiskDroppingList(c *rpc.Context) {
	ctx := c.Request.Context()
	span := trace.SpanFromContextSafe(ctx)
	span.Info("accept DiskDroppingList request")

	if err := s.raftNode.ReadIndex(ctx); err != nil {
		span.Errorf("read index error: %v", err)
		c.RespondError(apierrors.ErrRaftReadIndex)
		return
	}

	ret := &clustermgr.ListDiskRet{}
	var err error
	ret.Disks, err = s.DiskMgr.ListDroppingDisk(ctx)
	if err != nil {
		span.Errorf("list dropping disk failed => ", errors.Detail(err))
		err = errors.Info(apierrors.ErrUnexpected).Detail(err)
		c.RespondError(err)
		return
	}
	c.RespondJSON(ret)
}

// DiskHeartbeat blobnode通过disk/heartbeat接口上报本机所有disk的信息(主要是disk空间和chunk数)到CM，
// CM将[diskId、diskStatus、Readonly]响应给blobnode
func (s *Service) DiskHeartbeat(c *rpc.Context) {
	ctx := c.Request.Context()
	span := trace.SpanFromContextSafe(ctx)
	// 解析blobnode上报的本机所有disk信息
	args := new(clustermgr.DisksHeartbeatArgs)
	if err := c.ParseArgs(args); err != nil {
		c.RespondError(err)
		return
	}

	heartbeatDisks := make([]*blobnode.DiskHeartBeatInfo, 0)
	disks := make([]*clustermgr.DiskHeartbeatRet, len(args.Disks))
	for i := range args.Disks {
		info, err := s.DiskMgr.GetDiskInfo(ctx, args.Disks[i].DiskID)
		if err != nil {
			span.Errorf("get disk info %d failed, err: %v", args.Disks[i].DiskID, err)
			c.RespondError(err)
			return
		}
		disks[i] = &clustermgr.DiskHeartbeatRet{
			DiskID:   info.DiskID,
			Status:   info.Status,
			ReadOnly: info.Readonly,
		}

		// filter frequentHeatBeat disk
		// 剔除掉那些在心跳周期内刚刚上报过的disk，没必要再走一次raft将这些disk信息保存下来
		frequentHeatBeat, err := s.DiskMgr.IsFrequentHeatBeat(args.Disks[i].DiskID, s.HeartbeatNotifyIntervalS)
		if err != nil {
			span.Errorf("get disk info %d failed, err: %v", args.Disks[i].DiskID, err)
			c.RespondError(err)
			return
		}
		if !frequentHeatBeat {
			heartbeatDisks = append(heartbeatDisks, args.Disks[i])
		} else {
			span.Warnf("disk %d heartbeat too frequent", args.Disks[i].DiskID)
		}
	}

	ret := &clustermgr.DisksHeartbeatRet{Disks: disks}
	c.RespondJSON(ret)

	if len(heartbeatDisks) == 0 {
		return
	}
	args.Disks = heartbeatDisks
	data, err := json.Marshal(args)
	span.Debugf("heartbeat params: %s", string(data))
	if err != nil {
		span.Errorf("json marshal failed, args: %v, error: %v", args, err)
		err = errors.Info(apierrors.ErrUnexpected).Detail(err)
		c.RespondError(err)
		return
	}
	proposeInfo := base.EncodeProposeInfo(s.DiskMgr.GetModuleName(), diskmgr.OperTypeHeartbeatDiskInfo, data, base.ProposeContext{ReqID: span.TraceID()})
	err = s.raftNode.Propose(ctx, proposeInfo)
	if err != nil {
		span.Error("raft propose failed, err: ", err)
		c.RespondError(apierrors.ErrRaftPropose)
	}
}

// DiskAccess 磁盘切只读
func (s *Service) DiskAccess(c *rpc.Context) {
	ctx := c.Request.Context()
	span := trace.SpanFromContextSafe(ctx)
	args := new(clustermgr.DiskAccessArgs)
	if err := c.ParseArgs(args); err != nil {
		c.RespondError(err)
		return
	}
	span.Infof("accept DiskAccess request, args: %v", args)

	diskInfo, err := s.DiskMgr.GetDiskInfo(ctx, args.DiskID)
	if err != nil {
		c.RespondError(err)
		return
	}
	// 幂等性校验
	if diskInfo.Readonly == args.Readonly {
		return
	}

	// 判断磁盘是否正在走下线，正在走下线不允许切只读，因为已经在走下线了，切只读也没啥意义
	isDropping, err := s.DiskMgr.IsDroppingDisk(ctx, args.DiskID)
	if err != nil {
		c.RespondError(err)
		return
	}
	if isDropping {
		c.RespondError(apierrors.ErrDiskIsDropping)
		return
	}

	data, err := json.Marshal(args)
	if err != nil {
		span.Errorf("json marshal failed, args: %v, error: %v", args, err)
		c.RespondError(errors.Info(apierrors.ErrUnexpected).Detail(err))
		return
	}
	proposeInfo := base.EncodeProposeInfo(s.DiskMgr.GetModuleName(), diskmgr.OperTypeSwitchReadonly, data, base.ProposeContext{ReqID: span.TraceID()})
	err = s.raftNode.Propose(ctx, proposeInfo)
	if err != nil {
		span.Error("raft propose failed, err: ", err)
		c.RespondError(apierrors.ErrRaftPropose)
		return
	}

	// adjust volume health when setting disk readonly
	// 磁盘切只读最终也要影响到其上vuids所属volume的健康度
	err = s.VolumeMgr.DiskWritableChange(ctx, args.DiskID)
	if err != nil {
		span.Error("adjust volume health failed", errors.Detail(err))
		err = errors.Info(apierrors.ErrUnexpected).Detail(err)
		c.RespondError(err)
	}
}

func (s *Service) AdminDiskUpdate(c *rpc.Context) {
	ctx := c.Request.Context()
	span := trace.SpanFromContextSafe(ctx)
	args := new(blobnode.DiskInfo)
	if err := c.ParseArgs(args); err != nil {
		c.RespondError(err)
		return
	}
	span.Infof("accept DiskAccess request, args: %v", args)

	_, err := s.DiskMgr.GetDiskInfo(ctx, args.DiskID)
	if err != nil {
		span.Errorf("admin update disk:%d not exist", args.DiskID)
		c.RespondError(err)
		return
	}

	data, err := json.Marshal(args)
	if err != nil {
		span.Errorf("json marshal failed, args: %v, error: %v", args, err)
		c.RespondError(errors.Info(apierrors.ErrUnexpected).Detail(err))
		return
	}
	proposeInfo := base.EncodeProposeInfo(s.DiskMgr.GetModuleName(), diskmgr.OperTypeAdminUpdateDisk, data, base.ProposeContext{ReqID: span.TraceID()})
	err = s.raftNode.Propose(ctx, proposeInfo)
	if err != nil {
		span.Error("raft propose failed, err: ", err)
		c.RespondError(apierrors.ErrRaftPropose)
		return
	}
}
