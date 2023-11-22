package proto

import (
	"fmt"
	"time"

	"github.com/cubefs/cubefs/util/log"
)

const (
	CRRStatusEnabled  = "Enabled"
	CRRStatusDisabled = "Disabled"
)

type CRRConfiguration struct {
	VolName string     `json:"vol_name"`
	Rules   []*CRRRule `json:"rules"`
}

func (conf *CRRConfiguration) GetEnabledRuleTasks() []*CRRTask {
	tasks := make([]*CRRTask, 0)
	for _, r := range conf.Rules {
		if r.Status != RuleEnabled {
			log.LogDebugf("GetEnabledRuleTasks: skip disabled rule(%v) in volume(%v)", r.RuleID, conf.VolName)
			continue
		}
		task := &CRRTask{
			Id:      fmt.Sprintf("%s:%s", conf.VolName, r.RuleID),
			VolName: conf.VolName,
			Rule:    r,
		}
		tasks = append(tasks, task)
		log.LogDebugf("GetEnabledRuleTasks: RuleTask(%v) generated from rule(%v) in volume(%v)", *task, r.RuleID, conf.VolName)
	}
	return tasks
}

type CRRTask struct {
	Id      string
	VolName string
	Rule    *CRRRule
	SrcAuth Auth
	DstAuth Auth
}

type CRRRule struct {
	RuleID     string `json:"rule_id"`
	SrcVolName string `json:"src_vol_name"`
	DstVolName string `json:"dst_vol_name"`
	MasterAddr string `json:"master_addr"`
	SrcS3Addr  string `json:"src_s3_addr"`
	DstS3Addr  string `json:"dst_s3_addr"`
	Status     string `json:"status"`
	Prefix     string `json:"prefix"`
	SyncDelete bool   `json:"sync_delete"`
}

type Auth struct {
	AK string
	SK string
}

type CRRTaskRequest struct {
	MasterAddr string
	LcNodeAddr string
	Task       *CRRTask
}

type CRRTaskResponse struct {
	Id        string
	StartTime *time.Time
	EndTime   *time.Time
	Done      bool
	Status    uint8
	Result    string
	CRRTaskStatistic
}

type CRRTaskStatistic struct {
	Volume               string
	RuleId               string
	TotalInodeScannedNum int64
	FileScannedNum       int64
	DirScannedNum        int64
	ExpiredNum           int64
	ErrorSkippedNum      int64
}
