// Copyright 2018 The Chubao Authors.
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

package master

import (
	"fmt"
	"time"

	"github.com/cubefs/cubefs/util"
	"github.com/cubefs/cubefs/util/log"
)

func (c *Cluster) scheduleToCheckDiskRecoveryProgress() {
	go func() {
		for {
			if c.partition != nil && c.partition.IsRaftLeader() {
				if c.vols != nil {
					c.checkDiskRecoveryProgress()
				}
			}
			time.Sleep(time.Second * defaultIntervalToCheckDataPartition)
		}
	}()
}

func (c *Cluster) checkDiskRecoveryProgress() {
	defer func() {
		if r := recover(); r != nil {
			log.LogWarnf("checkDiskRecoveryProgress occurred panic,err[%v]", r)
			WarnBySpecialKey(fmt.Sprintf("%v_%v_scheduling_job_panic", c.Name, ModuleName),
				"checkDiskRecoveryProgress occurred panic")
		}
	}()

	c.badPartitionMutex.Lock()
	defer c.badPartitionMutex.Unlock()

	c.BadDataPartitionIds.Range(func(key, value interface{}) bool {
		badDataPartitionIds := value.([]uint64)
		newBadDpIds := make([]uint64, 0)
		for _, partitionID := range badDataPartitionIds {
			partition, err := c.getDataPartitionByID(partitionID)
			if err != nil {
				Warn(c.Name, fmt.Sprintf("checkDiskRecoveryProgress clusterID[%v],partitionID[%v] is not exist", c.Name, partitionID))
				continue
			}

			vol, err := c.getVol(partition.VolName)
			if err != nil {
				Warn(c.Name, fmt.Sprintf("checkDiskRecoveryProgress clusterID[%v],partitionID[%v] vol(%s) is not exist",
					c.Name, partitionID, partition.VolName))
				continue
			}

			if len(partition.Replicas) == 0 || len(partition.Replicas) < int(vol.dpReplicaNum) {
				newBadDpIds = append(newBadDpIds, partitionID)
				continue
			}

			if partition.getMinus() < util.GB {
				partition.isRecover = false
				partition.RLock()
				c.syncUpdateDataPartition(partition)
				partition.RUnlock()
				Warn(c.Name, fmt.Sprintf("clusterID[%v],partitionID[%v] has recovered success", c.Name, partitionID))
			} else {
				newBadDpIds = append(newBadDpIds, partitionID)
			}
		}

		if len(newBadDpIds) == 0 {
			Warn(c.Name, fmt.Sprintf("clusterID[%v],node:disk[%v] has recovered success", c.Name, key))
			c.BadDataPartitionIds.Delete(key)
		} else {
			c.BadDataPartitionIds.Store(key, newBadDpIds)
			log.LogInfof("BadDataPartitionIds key(%s) still have (%d) dp in recover", key, len(newBadDpIds))
		}

		return true
	})
}

func (c *Cluster) decommissionDisk(dataNode *DataNode, badDiskPath string, badPartitions []*DataPartition) (err error) {
	msg := fmt.Sprintf("action[decommissionDisk], Node[%v] OffLine,disk[%v]", dataNode.Addr, badDiskPath)
	log.LogWarn(msg)

	for _, dp := range badPartitions {
		go func(dp *DataPartition) {
			if err = c.decommissionDataPartition(dataNode.Addr, dp, diskOfflineErr); err != nil {
				return
			}
		}(dp)

	}
	msg = fmt.Sprintf("action[decommissionDisk],clusterID[%v] Node[%v] OffLine success",
		c.Name, dataNode.Addr)
	Warn(c.Name, msg)
	return
}
