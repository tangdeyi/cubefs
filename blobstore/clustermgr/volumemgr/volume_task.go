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
	"errors"
	"sync"

	"github.com/cubefs/cubefs/blobstore/api/blobnode"
	"github.com/cubefs/cubefs/blobstore/clustermgr/base"
	"github.com/cubefs/cubefs/blobstore/clustermgr/persistence/volumedb"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/util/log"
)

var errNotLeader = errors.New("this node is not leader")

func (m *VolumeMgr) reloadTasks() error {
	err := m.volumeTbl.ListTaskRecords(func(rec *volumedb.VolumeTaskRecord) bool {
		m.taskMgr.AddTask(newVolTask(rec.Vid, rec.TaskType, rec.TaskId, m.setVolumeStatus))
		m.lastTaskIdMap.Store(rec.Vid, rec.TaskId)
		return true
	})
	return err
}

func (m *VolumeMgr) setVolumeStatus(task *volTask) error {
	var (
		retErr  error
		wg      sync.WaitGroup
		once    sync.Once
		diskIds []proto.DiskID
		vid     proto.Vid
		vuids   []proto.Vuid
	)
	// 因为setVolumeStatus这个task是异步执行的，这里保证只有主节点才能去执行
	if !m.raftServer.IsLeader() {
		return errNotLeader
	}
	vol := m.all.getVol(task.vid)
	vol.lock.RLock()
	for _, v := range vol.vUnits {
		diskIds = append(diskIds, v.vuInfo.DiskID)
		vuids = append(vuids, proto.EncodeVuid(v.vuidPrefix, v.epoch))
	}
	vid = vol.vid
	vol.lock.RUnlock()

	if task.context == nil {
		task.context = make([]byte, len(diskIds))
	}
	span, ctx := trace.StartSpanFromContext(context.Background(), "")
	span.Infof("execute task %s", task.String())
	for i := 0; i < len(diskIds); i++ {
		if task.context[i] != 0 {
			continue
		}
		// 根据diskId拿到diskInfo，主要包含blobnode的host
		diskInfo, err := m.diskMgr.GetDiskInfo(ctx, diskIds[i])
		if err != nil {
			span.Errorf("failed to get disk info [task=%s vid=%d index=%d vuid=%d diskId=%d err=%v]",
				task.String(), vid, i, vuids[i], diskIds[i], err)
			return err
		}
		wg.Add(1)
		// send msg to blobnode
		// 同一个vid下的不同vuid并发调用blobnode来修改chunk状态
		go func(i int, host string, diskID proto.DiskID) {
			var (
				e   error
				msg string
			)
			defer wg.Done()
			arg := blobnode.ChangeChunkStatusArgs{
				DiskID: diskID,
				Vuid:   vuids[i],
			}
			switch task.taskType {
			case base.VolumeTaskTypeLock:
				msg = "readonly"
				e = m.blobNodeClient.SetChunkReadonly(ctx, host, &arg)
			case base.VolumeTaskTypeUnlock:
				msg = "readwrite"
				e = m.blobNodeClient.SetChunkReadwrite(ctx, host, &arg)
			default:
				log.Panicf("Unknown taskType(%d)", task.taskType)
			}
			if e == nil {
				span.Infof("set chunk %s [task=%s blobnode=%s vuid=%d] success", msg, task.String(), host, vuids[i])
				task.context[i] = 1 // 标识此chunk修改status成功
			} else {
				span.Errorf("set chunk %s [task=%s blobnode=%s vuid=%d] error: %v", msg, task.String(), host, vuids[i], e)
				// 有一个请求失败就认为任务失败，上层会加一些punish时间重试
				once.Do(func() {
					retErr = e
				})
			}
		}(i, diskInfo.Host, diskInfo.DiskID)
	}
	wg.Wait()
	if retErr != nil {
		return retErr
	}
	// delete task
	// task执行成功，走raft删除此task相关的信息
	if retErr = m.deleteTask(ctx, task); retErr != nil {
		span.Errorf("delete task %s error: %v", task.String(), retErr)
		return retErr
	}
	return nil
}

func (m *VolumeMgr) applyVolumeTask(ctx context.Context, vid proto.Vid, taskID string, t base.VolumeTaskType) error {
	// get volume from cache
	span := trace.SpanFromContextSafe(ctx)
	vol := m.all.getVol(vid)
	task := newVolTask(vid, t, taskID, m.setVolumeStatus)
	span.Infof("create task %s", task.String())
	// set volume status=lock if t is base.VolumeTaskTypeLock
	var (
		err        error
		taskRecord = &volumedb.VolumeTaskRecord{
			Vid:      vid,
			TaskType: task.taskType,
			TaskId:   task.taskId,
		}
	)
	switch t {
	// lock卷任务:先改卷状态，再在后台做异步调用blobnode
	case base.VolumeTaskTypeLock:
		vol.lock.Lock()
		// apply前再次判断volume是否可以lock
		if !vol.canLock() {
			span.Warnf("volume can't lock, status=%d", vol.getStatus())
			vol.lock.Unlock()
			return nil
		}
		// set volume status into lock, it'll call change volume status function
		// setStatus触发回调将该vid从idle列表中移除
		vol.setStatus(ctx, proto.VolumeStatusLock)
		rec := vol.ToRecord()
		// store task to db
		// 将vol和task信息持久化到volumeTbl
		err = m.volumeTbl.PutVolumeAndTask(rec, taskRecord)
		vol.lock.Unlock()
	// unlock卷任务
	case base.VolumeTaskTypeUnlock:
		vol.lock.Lock()
		if !vol.canUnlock() {
			span.Warnf("volume can't unlock, status=%d", vol.getStatus())
			vol.lock.Unlock()
			return nil
		}
		vol.setStatus(ctx, proto.VolumeStatusUnlocking)
		rec := vol.ToRecord()
		// store task to db
		err = m.volumeTbl.PutVolumeAndTask(rec, taskRecord)
		vol.lock.Unlock()
		// nothing to do
	default:
		span.Panicf("Unknown task type(%d)", t)
	}
	if err != nil {
		span.Errorf("persist task %s error: %v", task.String(), err)
		return err
	}

	// add task into taskManager
	// taskMgr异步调用setVolumeStatus(因为这里涉及到rpc调用blobnode修改chunk状态，可能会有超时重试，同步来做代价太大)
	m.lastTaskIdMap.Store(vid, task.taskId)
	m.taskMgr.AddTask(task)
	return nil
}

func (m *VolumeMgr) applyRemoveVolumeTask(ctx context.Context, vid proto.Vid, taskId string, taskType base.VolumeTaskType) error {
	span := trace.SpanFromContextSafe(ctx)
	vol := m.all.getVol(vid)
	if vol == nil {
		return ErrVolumeNotExist
	}
	// 获取到该vid对应的最新的taskId
	value, ok := m.lastTaskIdMap.Load(vid)
	if !ok {
		span.Infof("task[vid=%d taskId=%s type=%s] not found in last task map, this task maybe is deleted", vid, taskId, taskType.String())
		return nil
	}
	// 判断此task是否为该vid的最新task，有可能当前task不是最新的任务
	if value.(string) != taskId {
		span.Infof("task[vid=%d taskId=%s type=%s] in last task map, but the last taskId is %s, this task maybe update", vid, taskId, taskType.String(), value.(string))
		return nil
	}
	// 删除当前vid的对应的最新taskId
	m.lastTaskIdMap.Delete(vid)
	m.taskMgr.DeleteTask(vid, taskId) // follower should delete this task from task manager
	// unlock解锁卷任务做完，需要将卷的状态改回idle
	if taskType == base.VolumeTaskTypeUnlock {
		vol.lock.Lock()
		// set volume status into idle, it'll call change volume status function
		span.Debugf("vid: %d, status is: %s", vol.vid, vol.getStatus().String())
		vol.setStatus(ctx, proto.VolumeStatusIdle)
		// 修改完内存volume状态后，持久化volume信息到volumeTbl
		rec := vol.ToRecord()
		if err := m.volumeTbl.PutVolumeRecord(rec); err != nil {
			span.Errorf("delete task[vid=%d taskId=%s type=%s] error, update volume error: %v", vid, taskId, taskType.String(), err)
			vol.lock.Unlock()
			return err
		}
		vol.lock.Unlock()
	}
	// 删除持久化的task信息
	if err := m.volumeTbl.DeleteTaskRecord(vid); err != nil {
		span.Errorf("delete task[vid=%d taskId=%s type=%s] error, delete task record error: %v", vid, taskId, taskType.String(), err)
		return err
	}
	span.Infof("delete task[vid=%d taskId=%s type=%s] success", vid, taskId, taskType.String())
	return nil
}

func (m *VolumeMgr) deleteTask(ctx context.Context, task *volTask) error {
	span := trace.SpanFromContextSafe(ctx)
	data, err := json.Marshal(DeleteTaskCtx{
		Vid:      task.vid,
		TaskType: task.taskType,
		TaskId:   task.taskId,
	})
	if err != nil {
		return err
	}
	pr := base.EncodeProposeInfo(m.GetModuleName(), OperTypeDeleteTask, data, base.ProposeContext{ReqID: span.TraceID()})
	return m.raftServer.Propose(ctx, pr)
}

func (m *VolumeMgr) taskLoop() {
	m.taskMgr.run()
}
