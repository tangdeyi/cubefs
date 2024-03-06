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

package drive

import (
	"sync"
	"time"

	"github.com/cubefs/cubefs/apinode/sdk"
	"github.com/cubefs/cubefs/blobstore/common/rpc"
	"github.com/cubefs/cubefs/blobstore/util/taskpool"
)

type GetPropertiesResult = FileInfo

const (
	maxProperityNum = 1000
)

func (d *DriveNode) handleSetProperties(c *rpc.Context) {
	ctx, span := d.ctxSpan(c)
	args := new(ArgsProperties)
	if d.checkError(c, func(err error) { span.Error(err) }, c.ParseArgs(args), args.Path.Clean(false)) {
		return
	}

	uid := d.userID(c, &args.Path)
	xattrs, err := d.getProperties(c)
	if d.checkError(c, func(err error) { span.Info(err) }, err) {
		return
	}
	if len(xattrs) == 0 {
		c.Respond()
		return
	}
	if len(xattrs) > maxProperityNum {
		c.RespondError(sdk.ErrTooLarge)
		return
	}
	span.Info("to set xattrs:", xattrs)

	ur, vol, err := d.getUserRouterAndVolume(ctx, uid)
	if d.checkError(c, func(err error) { span.Errorf("get user router uid=%v error: %v", uid, err) }, err, ur.CanWrite()) {
		return
	}
	root := ur.RootFileID
	dirInfo, err := d.lookup(ctx, vol, root, args.Path.String())
	if d.checkError(c, func(err error) { span.Errorf("lookup path=%s error: %v", args.Path, err) }, err) {
		return
	}
	if d.checkFunc(c, func(err error) { span.Errorf("batch set xattr path=%s error: %v", args.Path, err) },
		func() error { return vol.BatchSetXAttr(ctx, dirInfo.Inode, xattrs) }) {
		return
	}
	c.Respond()
}

func (d *DriveNode) handleDelProperties(c *rpc.Context) {
	ctx, span := d.ctxSpan(c)
	args := new(ArgsProperties)
	if d.checkError(c, func(err error) { span.Error(err) }, c.ParseArgs(args), args.Path.Clean(false)) {
		return
	}

	uid := d.userID(c, &args.Path)
	xattrs, err := d.getProperties(c)
	if d.checkError(c, func(err error) { span.Info(err) }, err) {
		return
	}
	if len(xattrs) == 0 {
		c.Respond()
		return
	}
	if len(xattrs) > maxProperityNum {
		c.RespondError(sdk.ErrTooLarge)
		return
	}

	keys := make([]string, 0, len(xattrs))
	for k := range xattrs {
		keys = append(keys, k)
	}
	span.Info("to del xattrs:", keys)

	ur, vol, err := d.getUserRouterAndVolume(ctx, uid)
	if d.checkError(c, func(err error) { span.Errorf("get user router uid=%v error: %v", uid, err) }, err, ur.CanWrite()) {
		return
	}
	root := ur.RootFileID
	dirInfo, err := d.lookup(ctx, vol, root, args.Path.String())
	if d.checkError(c, func(err error) { span.Errorf("lookup path=%s error: %v", args.Path, err) }, err) {
		return
	}
	if d.checkFunc(c, func(err error) { span.Errorf("batch del xattr path=%s error: %v", args.Path, err) },
		func() error { return vol.BatchDeleteXAttr(ctx, dirInfo.Inode, keys) }) {
		return
	}
	c.Respond()
}

func (d *DriveNode) handleGetProperties(c *rpc.Context) {
	ctx, span := d.ctxSpan(c)
	args := new(ArgsProperties)
	if d.checkError(c, func(err error) { span.Error(err) }, c.ParseArgs(args), args.Path.Clean(false)) {
		return
	}

	uid := d.userID(c, &args.Path)
	ur, vol, err := d.getUserRouterAndVolume(ctx, uid)
	if d.checkError(c, func(err error) { span.Errorf("get user router uid=%v error: %v", uid, err) }, err) {
		return
	}
	root := ur.RootFileID
	dirInfo, err := d.lookup(ctx, vol, root, args.Path.String())
	if d.checkError(c, func(err error) { span.Errorf("lookup path=%s error: %v", args.Path, err) }, err) {
		return
	}
	st := time.Now()
	xattrs, err := vol.GetXAttrMap(ctx, dirInfo.Inode)
	span.AppendTrackLog("cpga", st, err)
	if d.checkError(c, func(err error) { span.Errorf("get xattr path=%s error: %v", args.Path, err) }, err) {
		return
	}
	st = time.Now()
	inoInfo, err := vol.GetInode(ctx, dirInfo.Inode)
	span.AppendTrackLog("cpgi", st, err)
	if d.checkError(c, func(err error) { span.Errorf("get inode path=%s error: %v", args.Path, err) }, err) {
		return
	}
	res := GetPropertiesResult{
		ID:         dirInfo.FileId,
		Path:       args.Path.String(),
		Type:       fileInfoType(dirInfo.IsDir()),
		Size:       int64(inoInfo.Size),
		Ctime:      inoInfo.CreateTime.Unix(),
		Mtime:      inoInfo.ModifyTime.Unix(),
		Atime:      inoInfo.AccessTime.Unix(),
		Properties: removeInternalMeta(xattrs),
	}
	d.respData(c, res)
}

func (d *DriveNode) handleBatchGetProperties(c *rpc.Context) {
	args := ArgsBatchPath{}
	ctx, span := d.ctxSpan(c)
	if d.checkError(c, func(err error) { span.Error(err) }, c.ParseArgs(&args)) {
		return
	}
	files := args.Paths
	for idx := range files {
		if err := files[idx].Clean(true); err != nil {
			d.respError(c, err)
			return
		}
	}
	uid := d.userID(c, nil)
	ur, vol, err := d.getUserRouterAndVolume(ctx, uid)
	if err != nil {
		d.respError(c, err)
		return
	}

	var respLock sync.Mutex
	resp := BatchUploadFileResult{
		Success: []FileInfo{},
		Errors:  []ErrorEntry{},
	}

	addErr := func(path string, e error) {
		rpcErr := rpc.Error2HTTPError(e)
		respLock.Lock()
		resp.Errors = append(resp.Errors, ErrorEntry{
			Path:    path,
			Status:  rpcErr.StatusCode(),
			Code:    rpcErr.ErrorCode(),
			Message: rpcErr.Error(),
		})
		respLock.Unlock()
	}

	pool := taskpool.New(8, 8)
	var wg sync.WaitGroup
	wg.Add(len(files))
	for i := range files {
		idx := i
		pool.Run(func() {
			defer wg.Done()

			path := files[idx].String()
			dirInfo, err := d.lookup(withNoTraceLog(ctx), vol, ur.RootFileID, path)
			if err != nil {
				addErr(path, err)
				return
			}
			xattrs, err := vol.GetXAttrMap(ctx, dirInfo.Inode)
			if err != nil {
				addErr(path, err)
				return
			}
			inoInfo, err := vol.GetInode(ctx, dirInfo.Inode)
			if err != nil {
				addErr(path, err)
				return
			}

			respLock.Lock()
			resp.Success = append(resp.Success, FileInfo{
				ID:         dirInfo.FileId,
				Path:       path,
				Type:       fileInfoType(dirInfo.IsDir()),
				Size:       int64(inoInfo.Size),
				Ctime:      inoInfo.CreateTime.Unix(),
				Mtime:      inoInfo.ModifyTime.Unix(),
				Atime:      inoInfo.AccessTime.Unix(),
				Properties: removeInternalMeta(xattrs),
			})
			respLock.Unlock()
		})
	}
	wg.Wait()
	pool.Close()

	span.Infof("batch get properties success(%d) error(%d)", len(resp.Success), len(resp.Errors))
	d.respData(c, resp)
}
