// Copyright 2023 The CubeFS Authors.
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

package proto

const (
	FlowLimit       string = "f"
	QPSLimit        string = "q"
	ConcurrentLimit string = "c"
)

type UserLimitConf struct {
	BandWidthQuota  map[string]uint64 `json:"band_width_quota"` // uid --> BytesPS
	QPSQuota        map[string]uint64 `json:"qps_quota"`        // uid --> QPS
	ConcurrentQuota map[string]uint64 `json:"concurrent_quota"` // uid --> concurrency
}
