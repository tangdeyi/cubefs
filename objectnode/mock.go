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

package objectnode

//go:generate mockgen -build_flags=--mod=mod -destination=volumemgr_mock.go -package=objectnode -mock_names VolumeMgrAPI=MockVolumeMgrAPI github.com/cubefs/cubefs/objectnode VolumeMgrAPI
//go:generate mockgen -build_flags=--mod=mod -destination=mastermgr_mock.go -package=objectnode -mock_names MasterMgrAPI=MockMasterMgrAPI github.com/cubefs/cubefs/objectnode MasterMgrAPI
//go:generate mockgen -build_flags=--mod=mod -destination=datamgr_mock.go -package=objectnode -mock_names ExtentClientAPI=MockExtentClientAPI github.com/cubefs/cubefs/objectnode ExtentClientAPI
//go:generate mockgen -build_flags=--mod=mod -destination=metamgr_mock.go -package=objectnode -mock_names MetaClientAPI=MockMetaClientAPI github.com/cubefs/cubefs/objectnode MetaClientAPI

//go:generate mockgen -build_flags=--mod=mod -destination=adminapi_mock.go -package=objectnode -mock_names AdminAPIInterface=MockAdminAPIInterface github.com/cubefs/cubefs/sdk/master AdminAPIInterface
//go:generate mockgen -build_flags=--mod=mod -destination=clientapi_mock.go -package=objectnode -mock_names ClientAPIInterface=MockClientAPIInterface github.com/cubefs/cubefs/sdk/master ClientAPIInterface
//go:generate mockgen -build_flags=--mod=mod -destination=nodeapi_mock.go -package=objectnode -mock_names NodeAPIInterface=MockNodeAPIInterface github.com/cubefs/cubefs/sdk/master NodeAPIInterface
//go:generate mockgen -build_flags=--mod=mod -destination=userapi_mock.go -package=objectnode -mock_names UserAPIInterface=MockUserAPIInterface github.com/cubefs/cubefs/sdk/master UserAPIInterface
