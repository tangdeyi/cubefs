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

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"syscall"
	"testing"

	"github.com/cubefs/cubefs/proto"
	"github.com/golang/mock/gomock"
	"github.com/gorilla/mux"
	"github.com/stretchr/testify/require"
)

type mockUserInfoStore struct {
}

func (u *mockUserInfoStore) LoadUser(accessKey string) (*proto.UserInfo, error) {
	return new(proto.UserInfo), nil
}

type mockObjectServer struct {
	*ObjectNode
	*httptest.Server
}

func createObjectServer(t *testing.T) *mockObjectServer {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockMetaClientAPI := NewMockMetaClientAPI(ctrl)
	mockMetaClientAPI.EXPECT().Lookup_ll(gomock.Any(), gomock.Any()).AnyTimes().DoAndReturn(mockLookup_ll)
	mockMetaClientAPI.EXPECT().InodeCreate_ll(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		AnyTimes().DoAndReturn(mockInodeCreate_ll)
	mockMetaClientAPI.EXPECT().InodeGet_ll(gomock.Any()).AnyTimes().DoAndReturn(mockInodeGet_ll)
	mockMetaClientAPI.EXPECT().Create_ll(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		AnyTimes().DoAndReturn(mockCreate_ll)
	mockMetaClientAPI.EXPECT().BatchSetXAttr_ll(gomock.Any(), gomock.Any()).AnyTimes()
	mockMetaClientAPI.EXPECT().DentryCreate_ll(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes()

	mockExtentClientAPI := NewMockExtentClientAPI(ctrl)
	mockExtentClientAPI.EXPECT().OpenStream(gomock.Any()).AnyTimes()
	mockExtentClientAPI.EXPECT().Write(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes().DoAndReturn(mockWrite)
	mockExtentClientAPI.EXPECT().Flush(gomock.Any()).AnyTimes()
	mockExtentClientAPI.EXPECT().CloseStream(gomock.Any()).AnyTimes()

	mockVolumeAPI := NewMockVolumeMgrAPI(ctrl)
	mockVolumeAPI.EXPECT().Volume(gomock.Any()).AnyTimes().DoAndReturn(
		func(bucket string) (*Volume, error) {
			return &Volume{
				mw:   mockMetaClientAPI,
				ec:   mockExtentClientAPI,
				name: bucket,
			}, nil
		})

	mockMasterMgrAPI := NewMockMasterMgrAPI(ctrl)
	mockUserInfoStore := &mockUserInfoStore{}
	router := mux.NewRouter().SkipClean(true)
	objectNode := &ObjectNode{
		vm:        mockVolumeAPI,
		mc:        mockMasterMgrAPI,
		userStore: mockUserInfoStore,
	}
	objectNode.registerApiRouters(router)
	testServer := httptest.NewServer(router)
	mos := &mockObjectServer{
		objectNode,
		testServer,
	}
	return mos
}

func mockLookup_ll(parentID uint64, name string) (inode uint64, mode uint32, err error) {
	return 0, 0, syscall.ENOENT
}

func mockInodeCreate_ll(mode, uid, gid uint32, target []byte, quotaIds []uint64) (*proto.InodeInfo, error) {
	inode := new(proto.InodeInfo)
	inode.Inode = 100
	return inode, nil
}

func mockCreate_ll(parentID uint64, name string, mode, uid, gid uint32, target []byte) (*proto.InodeInfo, error) {
	if name == "path" {
		mode = uint32(DefaultDirMode)
	} else {
		mode = uint32(DefaultFileMode)
	}
	inode := new(proto.InodeInfo)
	inode.Inode = 100
	inode.Mode = mode
	return inode, nil
}

func mockInodeGet_ll(inode uint64) (*proto.InodeInfo, error) {
	inodoInfo := new(proto.InodeInfo)
	inodoInfo.Inode = 100
	return inodoInfo, nil
}

func mockWrite(inode uint64, offset int, data []byte, flags int, checkFunc func() error) (write int, err error) {
	return len(data), nil
}

func Test_PutObject(t *testing.T) {

	objectServer := createObjectServer(t)
	defer objectServer.Server.Close()

	req, err := http.NewRequest(http.MethodPut, objectServer.URL+"/bucket/path/a%2Bb.txt", strings.NewReader("test"))
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	req.Header.Set("cache-control", "public")
	req.Header.Set("content-disposition", `attachment; filename="fname.ext"`)
	req.Header.Set("content-encoding", "ce")
	req.Header.Set("content-md5", "CY9rzUYh03PK3k6DJie09g==")
	req.Header.Set("content-type", "application/json")
	req.Header.Set("expires", "Mon, 02 Jan 2026 15:04:05 GMT")
	req.Header.Set("x-amz-storage-class", "STANDARD_IA")
	req.Header.Set("x-amz-meta-a", "aa")
	req.Header.Set("x-amz-meta-b", "bb")
	req.Header.Set("x-amz-website-redirect-location", "/url")
	c := &http.Client{}
	resp, err := c.Do(req)
	require.NoError(t, err)
	require.Equal(t, 200, resp.StatusCode)
	require.Equal(t, `"098f6bcd4621d373cade4e832627b4f6"`, resp.Header.Get("etag"))

}
