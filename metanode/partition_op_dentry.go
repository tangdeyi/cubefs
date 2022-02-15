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

package metanode

import (
	"encoding/json"
	"fmt"

	"github.com/cubefs/cubefs/proto"
)

// CreateDentry returns a new dentry.
func (mp *metaPartition) CreateDentry(req *CreateDentryReq, p *Packet) (err error) {
	if req.ParentID == req.Inode {
		err = fmt.Errorf("parentId is equal inodeId")
		p.PacketErrorWithBody(proto.OpExistErr, []byte(err.Error()))
		return
	}

	dentry := &Dentry{
		ParentId: req.ParentID,
		Name:     req.Name,
		Inode:    req.Inode,
		Type:     req.Mode,
	}
	val, err := dentry.Marshal()
	if err != nil {
		return
	}
	resp, err := mp.submit(opFSMCreateDentry, val)
	if err != nil {
		p.PacketErrorWithBody(proto.OpAgain, []byte(err.Error()))
		return
	}
	p.ResultCode = resp.(uint8)
	return
}

// DeleteDentry deletes a dentry.
func (mp *metaPartition) DeleteDentry(req *DeleteDentryReq, p *Packet) (err error) {
	dentry := &Dentry{
		ParentId: req.ParentID,
		Name:     req.Name,
	}
	val, err := dentry.Marshal()
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
		return
	}
	r, err := mp.submit(opFSMDeleteDentry, val)
	if err != nil {
		p.PacketErrorWithBody(proto.OpAgain, []byte(err.Error()))
		return
	}
	retMsg := r.(*DentryResponse)
	p.ResultCode = retMsg.Status
	dentry = retMsg.Msg
	if p.ResultCode == proto.OpOk {
		var reply []byte
		resp := &DeleteDentryResp{
			Inode: dentry.Inode,
		}
		reply, err = json.Marshal(resp)
		p.PacketOkWithBody(reply)
	}
	return
}

// DeleteDentry deletes a dentry.
func (mp *metaPartition) DeleteDentryBatch(req *BatchDeleteDentryReq, p *Packet) (err error) {

	db := make(DentryBatch, 0, len(req.Dens))

	for _, d := range req.Dens {
		db = append(db, &Dentry{
			ParentId: req.ParentID,
			Name:     d.Name,
			Inode:    d.Inode,
			Type:     d.Type,
		})
	}

	val, err := db.Marshal()
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
		return
	}
	r, err := mp.submit(opFSMDeleteDentryBatch, val)
	if err != nil {
		p.PacketErrorWithBody(proto.OpAgain, []byte(err.Error()))
		return err
	}

	retMsg := r.([]*DentryResponse)
	p.ResultCode = proto.OpOk

	bddr := &BatchDeleteDentryResp{}

	for _, m := range retMsg {
		if m.Status != proto.OpOk {
			p.ResultCode = proto.OpErr
		}

		if dentry := m.Msg; dentry != nil {
			bddr.Items = append(bddr.Items, &struct {
				Inode  uint64 `json:"ino"`
				Status uint8  `json:"status"`
			}{
				Inode:  dentry.Inode,
				Status: m.Status,
			})
		} else {
			bddr.Items = append(bddr.Items, &struct {
				Inode  uint64 `json:"ino"`
				Status uint8  `json:"status"`
			}{
				Status: m.Status,
			})
		}

	}

	reply, err := json.Marshal(bddr)
	if err != nil {
		p.PacketErrorWithBody(proto.OpAgain, []byte(err.Error()))
		return err
	}
	p.PacketOkWithBody(reply)

	return
}

// UpdateDentry updates a dentry.
func (mp *metaPartition) UpdateDentry(req *UpdateDentryReq, p *Packet) (err error) {
	if req.ParentID == req.Inode {
		err = fmt.Errorf("parentId is equal inodeId")
		p.PacketErrorWithBody(proto.OpExistErr, []byte(err.Error()))
		return
	}

	dentry := &Dentry{
		ParentId: req.ParentID,
		Name:     req.Name,
		Inode:    req.Inode,
	}
	val, err := dentry.Marshal()
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
		return
	}
	resp, err := mp.submit(opFSMUpdateDentry, val)
	if err != nil {
		p.PacketErrorWithBody(proto.OpAgain, []byte(err.Error()))
		return
	}
	msg := resp.(*DentryResponse)
	p.ResultCode = msg.Status
	if msg.Status == proto.OpOk {
		var reply []byte
		m := &UpdateDentryResp{
			Inode: msg.Msg.Inode,
		}
		reply, err = json.Marshal(m)
		p.PacketOkWithBody(reply)
	}
	return
}

func (mp *metaPartition) ReadDirOnly(req *ReadDirOnlyReq, p *Packet) (err error) {
	resp := mp.readDirOnly(req)
	reply, err := json.Marshal(resp)
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
		return
	}
	p.PacketOkWithBody(reply)
	return
}

// ReadDir reads the directory based on the given request.
func (mp *metaPartition) ReadDir(req *ReadDirReq, p *Packet) (err error) {
	resp := mp.readDir(req)
	reply, err := json.Marshal(resp)
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
		return
	}
	p.PacketOkWithBody(reply)
	return
}

func (mp *metaPartition) ReadDirLimit(req *ReadDirLimitReq, p *Packet) (err error) {
	resp := mp.readDirLimit(req)
	reply, err := json.Marshal(resp)
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
		return
	}
	p.PacketOkWithBody(reply)
	return
}

// Lookup looks up the given dentry from the request.
func (mp *metaPartition) Lookup(req *LookupReq, p *Packet) (err error) {
	dentry := &Dentry{
		ParentId: req.ParentID,
		Name:     req.Name,
	}
	dentry, status := mp.getDentry(dentry)
	var reply []byte
	if status == proto.OpOk {
		resp := &LookupResp{
			Inode: dentry.Inode,
			Mode:  dentry.Type,
		}
		reply, err = json.Marshal(resp)
		if err != nil {
			status = proto.OpErr
			reply = []byte(err.Error())
		}
	}
	p.PacketErrorWithBody(status, reply)
	return
}

// GetDentryTree returns the dentry tree stored in the meta partition.
func (mp *metaPartition) GetDentryTree() *BTree {
	return mp.dentryTree.GetTree()
}
