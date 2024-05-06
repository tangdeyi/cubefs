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

package raftserver

import (
	"bytes"
	"context"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cubefs/cubefs/blobstore/common/raftserver/wal"
	"github.com/cubefs/cubefs/blobstore/util/log"
	"go.etcd.io/etcd/raft/v3"
	pb "go.etcd.io/etcd/raft/v3/raftpb"
)

const (
	applyChCapacity = 512
	applyChLength   = 400
)

type RaftServer interface {
	Stop()
	Propose(ctx context.Context, data []byte) error
	ReadIndex(ctx context.Context) error
	TransferLeadership(ctx context.Context, leader, transferee uint64)
	AddMember(ctx context.Context, member Member) error
	RemoveMember(ctx context.Context, nodeID uint64) error
	IsLeader() bool
	Status() Status

	// In order to prevent log expansion, the application needs to call this method.
	Truncate(index uint64) error
}

// apply contains entries, snapshot to be applied. Once
// an apply is consumed, the entries will be persisted to
// to raft storage concurrently; the application must read
// raftDone before assuming the raft messages are stable.
type apply struct {
	entries  []pb.Entry
	snapshot pb.Snapshot
	notifyc  chan struct{}
}

type raftServer struct {
	cfg            Config
	proposeTimeout time.Duration
	tickInterval   time.Duration
	snapTimeout    time.Duration
	lead           uint64 // 当前leader的NodeId
	n              raft.Node
	shotter        *snapshotter
	/*storage负责log entry存储，raft和storage联系很紧密，因此raft提供了storage接口，由用户实现，
	然后在启动时设置到Config中，storage接口只涉及查询操作，何时持久化和如何持久化由用户决定*/
	store        *raftStorage
	idGen        *Generator   // id生成器，notify map依赖这个id
	sm           StateMachine // 上层业务实现log entry的apply状态机，即CM的Service
	readNotifier atomic.Value
	notifiers    sync.Map
	/*
		transportation 并没有在 etcd/raft 中处理，而是全权交给用户处理，需要发送的消息都会存放在 raft.msgs 中。
		Ready 结构体包含自上次处理后已就绪的需要由用户处理的信息， 包括需要发送的 msg 和需要持久化存储的信息。
		在 goroutine 每次循环开始会对比之前的状态与当前状态，生成 Ready， node.Ready() 方法就是返回对应的 readyc。
		由用户对 Ready 处理，从而实现了分层：
		1. 持久化 Entries、HardState、Snapshot；
		2. 发送 Messages 给其他节点；
		3. 应用 snapshot、CommittedEntries 到状态机；
		4. 调用 node.Advance() 通知 raft 之前的更新已处理完，可以进行清理，同时可以处理下一轮更新
	*/
	tr Transport
	/**/
	applyWait  WaitTime
	propc      chan propose
	readStateC chan raft.ReadState
	applyc     chan apply
	snapshotC  chan Snapshot
	snapMsgc   chan pb.Message
	readwaitc  chan struct{}
	stopc      chan struct{}
	once       sync.Once
}

func NewRaftServer(cfg *Config) (RaftServer, error) {
	if err := cfg.Verify(); err != nil {
		return nil, err
	}
	tickInterval := time.Duration(cfg.TickInterval) * time.Second
	if cfg.TickIntervalMs > 0 {
		tickInterval = time.Duration(cfg.TickIntervalMs) * time.Millisecond
	}
	proposeTimeout := time.Duration(cfg.ProposeTimeout) * time.Second
	snapTimeout := time.Duration(cfg.SnapshotTimeout) * time.Second
	rs := &raftServer{
		cfg:            *cfg,
		proposeTimeout: proposeTimeout,
		tickInterval:   tickInterval,
		snapTimeout:    snapTimeout,
		shotter:        newSnapshotter(cfg.MaxSnapConcurrency, snapTimeout),
		idGen:          NewGenerator(cfg.NodeId, time.Now()),
		sm:             cfg.SM,
		applyWait:      NewTimeList(),
		propc:          make(chan propose, 512),
		readStateC:     make(chan raft.ReadState, 64),
		applyc:         make(chan apply, applyChCapacity),
		snapshotC:      make(chan Snapshot),
		snapMsgc:       make(chan pb.Message, cfg.MaxSnapConcurrency),
		readwaitc:      make(chan struct{}),
		stopc:          make(chan struct{}),
	}
	rs.readNotifier.Store(newReadIndexNotifier())

	begin := time.Now()
	store, err := NewRaftStorage(cfg.WalDir, cfg.WalSync, cfg.NodeId, rs.sm, rs.shotter)
	if err != nil {
		return nil, err
	}
	lastIndex, _ := store.LastIndex()
	firstIndex, _ := store.FirstIndex()
	hs, _, err := store.InitialState()
	if err != nil {
		return nil, err
	}

	log.Infof("load raft wal success, total: %dus, firstIndex: %d, lastIndex: %d, members: %+v",
		time.Since(begin).Microseconds(), firstIndex, lastIndex, cfg.Members)

	rs.store = store
	raftCfg := &raft.Config{
		ID:              cfg.NodeId,
		ElectionTick:    cfg.ElectionTick,
		HeartbeatTick:   cfg.HeartbeatTick,
		Storage:         store,
		MaxSizePerMsg:   64 * 1024 * 1024,
		MaxInflightMsgs: 1024,
		CheckQuorum:     true,
		PreVote:         true,
		Logger:          log.DefaultLogger,
	}
	rs.tr = NewTransport(cfg.ListenPort, rs)
	for _, m := range cfg.Members {
		rs.addMember(m)
	}
	if hs.Commit < cfg.Applied {
		cfg.Applied = hs.Commit
	}
	raftCfg.Applied = cfg.Applied
	store.SetApplied(cfg.Applied)
	rs.n = raft.RestartNode(raftCfg)

	go rs.raftStart()
	go rs.raftApply()
	go rs.run()
	go rs.linearizableReadLoop()
	return rs, nil
}

func (s *raftServer) Stop() {
	s.once.Do(func() {
		s.tr.Stop()
		s.n.Stop()
		close(s.stopc)
		s.shotter.Stop()
		s.store.Close()
	})
}

func (s *raftServer) Propose(ctx context.Context, data []byte) (err error) {
	id := s.idGen.Next()
	return s.propose(ctx, id, pb.EntryNormal, normalEntryEncode(id, data))
}

func (s *raftServer) propose(ctx context.Context, id uint64, entryType pb.EntryType, data []byte) (err error) {
	pr := propose{
		nr:        newNotifier(),
		id:        id,
		entryType: entryType,
		b:         data,
	}

	s.notifiers.Store(id, pr.nr)
	defer func() {
		s.notifiers.Delete(id)
	}()

	ctx, cancel := context.WithTimeout(ctx, s.proposeTimeout)
	defer cancel()
	select {
	case s.propc <- pr:
	case <-ctx.Done():
		err = ctx.Err()
		return
	case <-s.stopc:
		err = ErrStopped
		return
	}

	// 等apply完成再返回
	return pr.nr.wait(ctx, s.stopc)
}

func (s *raftServer) IsLeader() bool {
	return atomic.LoadUint64(&s.lead) == s.cfg.NodeId
}

func (s *raftServer) ReadIndex(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, s.proposeTimeout)
	defer cancel()
	// wait for read state notification
	nr := s.readNotifier.Load().(*readIndexNotifier)
	select {
	case s.readwaitc <- struct{}{}:
	default:
	}
	return nr.Wait(ctx, s.stopc)
}

func (s *raftServer) TransferLeadership(ctx context.Context, leader, transferee uint64) {
	s.n.TransferLeadership(ctx, leader, transferee)
}

func (s *raftServer) changeMember(ctx context.Context, cc pb.ConfChange) (err error) {
	data, err := cc.Marshal()
	if err != nil {
		return err
	}
	return s.propose(ctx, cc.ID, pb.EntryConfChange, data)
}

func (s *raftServer) AddMember(ctx context.Context, member Member) (err error) {
	body, err := member.Marshal()
	if err != nil {
		return err
	}
	addType := pb.ConfChangeAddNode
	if member.Learner {
		addType = pb.ConfChangeAddLearnerNode
	}
	id := s.idGen.Next()
	cc := pb.ConfChange{
		ID:      id,
		Type:    addType,
		NodeID:  member.NodeID,
		Context: body,
	}
	return s.changeMember(ctx, cc)
}

func (s *raftServer) RemoveMember(ctx context.Context, peerId uint64) (err error) {
	id := s.idGen.Next()
	cc := pb.ConfChange{
		ID:     id,
		Type:   pb.ConfChangeRemoveNode,
		NodeID: peerId,
	}
	return s.changeMember(ctx, cc)
}

func (s *raftServer) Status() Status {
	st := s.n.Status()
	status := Status{
		Id:             st.ID,
		Term:           st.Term,
		Vote:           st.Vote,
		Commit:         st.Commit,
		Leader:         st.Lead,
		RaftState:      st.RaftState.String(),
		Applied:        s.store.Applied(),
		RaftApplied:    st.Applied,
		ApplyingLength: len(s.applyc),
		LeadTransferee: st.LeadTransferee,
	}
	for id, pr := range st.Progress {
		var host string
		if m, ok := s.store.GetMember(id); ok {
			host = m.Host
		}
		peer := Peer{
			Id:              id,
			Host:            host,
			Match:           pr.Match,
			Next:            pr.Next,
			State:           pr.State.String(),
			Paused:          pr.IsPaused(),
			PendingSnapshot: pr.PendingSnapshot,
			RecentActive:    pr.RecentActive,
			IsLearner:       pr.IsLearner,
			InflightFull:    pr.Inflights.Full(),
			InflightCount:   pr.Inflights.Count(),
		}
		status.Peers = append(status.Peers, peer)
	}
	return status
}

func (s *raftServer) Truncate(index uint64) error {
	return s.store.Truncate(index)
}

func (s *raftServer) campaign(ctx context.Context) error {
	return s.n.Campaign(ctx)
}

func (s *raftServer) notify(id uint64, err error) {
	val, hit := s.notifiers.Load(id)
	if !hit {
		return
	}
	val.(notifier).notify(err)
}

func (s *raftServer) getSnapshot(name string) *snapshot {
	return s.shotter.Get(name)
}

func (s *raftServer) reportSnapshot(to uint64, status raft.SnapshotStatus) {
	s.n.ReportSnapshot(to, status)
}

func (s *raftServer) deleteSnapshot(name string) {
	s.shotter.Delete(name)
}

func (s *raftServer) raftApply() {
	var notifies []chan struct{}
	for {
		select {
		case ap := <-s.applyc:
			entries := ap.entries
			snap := ap.snapshot
			notifies = append(notifies, ap.notifyc)
			n := len(s.applyc)
			// if applyc length bigger than 80% of applyChCapacity, record the length in log
			if n > applyChLength {
				log.Warnf("applyc capacity is %d,now length is:%d, over %d", applyChCapacity, n, applyChLength)
			}
			for i := 0; i < n && raft.IsEmptySnap(snap); i++ {
				ap = <-s.applyc
				entries = append(entries, ap.entries...)
				snap = ap.snapshot
				notifies = append(notifies, ap.notifyc)
			}
			// 日志在这里被应用到状态机
			s.applyEntries(entries)
			s.applySnapshotFinish(snap)
			s.applyWait.Trigger(s.store.Applied())
			for _, notifyc := range notifies {
				<-notifyc
			}
			notifies = notifies[0:0]
		case snapMsg := <-s.snapMsgc:
			go s.processSnapshotMessage(snapMsg)
		case snap := <-s.snapshotC:
			s.applySnapshot(snap)
		case <-s.stopc:
			return
		}
	}
}

func (s *raftServer) applyConfChange(entry pb.Entry) {
	var cc pb.ConfChange
	if err := cc.Unmarshal(entry.Data); err != nil {
		log.Panicf("unmarshal confchange error: %v", err)
		return
	}
	if entry.Index <= s.store.Applied() {
		s.notify(cc.ID, nil)
		return
	}
	switch cc.Type {
	case pb.ConfChangeAddNode, pb.ConfChangeAddLearnerNode:
		var member Member
		if err := member.Unmarshal(cc.Context); err != nil {
			log.Panicf("failed to unmarshal context that in conf change, error: %v", err)
		}
		s.addMember(member)
	case pb.ConfChangeRemoveNode:
		s.removeMember(cc.NodeID)
	}
	s.n.ApplyConfChange(cc)
	if err := s.sm.ApplyMemberChange(ConfChange(cc), entry.Index); err != nil {
		log.Panicf("application sm apply member change error: %v", err)
	}

	s.notify(cc.ID, nil)
}

func (s *raftServer) applyEntries(entries []pb.Entry) {
	var (
		prIds        []uint64
		pendinsDatas [][]byte
		lastIndex    uint64
	)
	if len(entries) == 0 {
		return
	}
	for _, ent := range entries {
		switch ent.Type {
		case pb.EntryConfChange:
			if len(pendinsDatas) > 0 {
				if err := s.sm.Apply(pendinsDatas, lastIndex); err != nil {
					log.Panicf("StateMachine apply error: %v", err)
				}
				for i := 0; i < len(prIds); i++ {
					s.notify(prIds[i], nil)
				}
				pendinsDatas = pendinsDatas[0:0]
				prIds = prIds[0:0]
			}
			s.applyConfChange(ent)
		case pb.EntryNormal:
			if len(ent.Data) == 0 {
				continue
			}
			id, data := normalEntryDecode(ent.Data)
			if ent.Index <= s.store.Applied() { // this message should be ignored
				s.notify(id, nil)
				continue
			}
			pendinsDatas = append(pendinsDatas, data)
			prIds = append(prIds, id)
			lastIndex = ent.Index
		}
	}

	if len(pendinsDatas) > 0 {
		// 应用到状态机
		if err := s.sm.Apply(pendinsDatas, lastIndex); err != nil {
			log.Panicf("StateMachine apply error: %v", err)
		}
		// notify：apply完成通知调用方(比如propose那里)
		for i := 0; i < len(prIds); i++ {
			s.notify(prIds[i], nil)
		}
	}

	if len(entries) > 0 {
		// save applied id
		s.store.SetApplied(entries[len(entries)-1].Index)
	}
}

func (s *raftServer) applySnapshotFinish(st pb.Snapshot) {
	if raft.IsEmptySnap(st) {
		return
	}
	log.Infof("node[%d] apply snapshot[meta: %s, name: %s]", s.cfg.NodeId, st.Metadata.String(), string(st.Data))
	walSnap := wal.Snapshot{Index: st.Metadata.Index, Term: st.Metadata.Term}
	if err := s.store.ApplySnapshot(walSnap); err != nil {
		log.Panicf("apply snapshot error: %v", err)
	}
}

func (s *raftServer) applySnapshot(snap Snapshot) {
	meta := snap.(*applySnapshot).meta
	nr := snap.(*applySnapshot).nr
	log.Infof("apply snapshot(%s) data......", meta.Name)
	// read snapshot data
	if err := s.sm.ApplySnapshot(meta, snap); err != nil {
		log.Errorf("apply snapshot(%s) error: %v", meta.Name, err)
		nr.notify(err)
		return
	}
	log.Infof("apply snapshot(%s) success", meta.Name)
	s.updateMembers(meta.Mbs)
	s.store.SetApplied(meta.Index)
	nr.notify(nil)
}

func (s *raftServer) processSnapshotMessage(m pb.Message) {
	name := string(m.Snapshot.Data)
	st := s.getSnapshot(name)
	if st == nil {
		log.Errorf("not found snapshot(%s)", name)
		s.reportSnapshot(m.To, raft.SnapshotFailure)
		return
	}
	defer s.deleteSnapshot(name)
	if err := s.tr.SendSnapshot(m.To, st); err != nil {
		s.reportSnapshot(m.To, raft.SnapshotFailure)
		log.Errorf("send snapshot(%s) to node(%d) error: %v", name, m.To, err)
		return
	}
	s.reportSnapshot(m.To, raft.SnapshotFinish)
	// send snapshot message to m.TO
	s.tr.Send([]pb.Message{m})
}

// 重要的raft状态机循环
func (s *raftServer) raftStart() {
	ticker := time.NewTicker(s.tickInterval)
	defer ticker.Stop()
	for {
		select {
		case <-s.stopc:
			return
		case <-ticker.C:
			s.n.Tick() // 逻辑时钟递增，心跳间隔和选举超时时间都以这个为单位
		case rd := <-s.n.Ready(): // 处理Ready结构体，包含msgs和Entry
			if rd.SoftState != nil { // SoftState在节点状态更新时不为空（发生切主/启动时选主）
				leader := atomic.SwapUint64(&s.lead, rd.SoftState.Lead)
				if rd.SoftState.Lead != leader {
					var leaderHost string
					if m, ok := s.store.GetMember(rd.SoftState.Lead); ok {
						leaderHost = m.Host
					}
					// leader change变更
					s.sm.LeaderChange(rd.SoftState.Lead, leaderHost)
				}
			}
			isLeader := s.IsLeader()

			// readIndex请求后返回此字段，Index返回的是readIndex请求时的CommitIndex，RequestCtx是readIndex请求时的参数rctx
			if len(rd.ReadStates) != 0 {
				select {
				case s.readStateC <- rd.ReadStates[len(rd.ReadStates)-1]:
				case <-s.stopc:
					return
				default:
					log.Warn("read state chan is not ready!!!")
				}
			}

			notifyc := make(chan struct{}, 1)
			ap := apply{
				entries:  rd.CommittedEntries, // 已经committed到raftStorage的Entries，还没有被apply
				snapshot: rd.Snapshot,
				notifyc:  notifyc,
			}

			select {
			case s.applyc <- ap: // 发送消息给后台apply协程
			case <-s.stopc:
				return
			}

			// 发送 msg 给其他节点，raft节点的通信都是通过 msg 来的
			// raft所有的处理都抽象为 msg，最终通过 Step 接口处理
			if isLeader {
				s.tr.Send(s.processMessages(rd.Messages))
			}

			// 持久化Entries
			if len(rd.Entries) > 0 {
				err := s.store.SaveEntries(rd.Entries)
				if err != nil {
					log.Panicf("save raft entries error: %v", err)
				}
			}
			// 持久化HardState，Commit、Vote、Term
			if !raft.IsEmptyHardState(rd.HardState) {
				if err := s.store.SaveHardState(rd.HardState); err != nil {
					log.Panicf("save raft hardstate error: %v", err)
				}
			}

			if !isLeader {
				msgs := s.processMessages(rd.Messages)
				notifyc <- struct{}{}
				waitApply := false
				for _, ent := range rd.CommittedEntries {
					if ent.Type == pb.EntryConfChange {
						waitApply = true
						break
					}
				}
				if waitApply {
					select {
					case notifyc <- struct{}{}:
					case <-s.stopc:
						return
					}
				}
				s.tr.Send(msgs)
			} else {
				notifyc <- struct{}{}
			}

			// 通知node上一批从Ready拿出来的消息已经处理完成，应用完上一批从Ready的消息都应该调用Advance
			s.n.Advance()
		}
	}
}

func (s *raftServer) processMessages(ms []pb.Message) []pb.Message {
	sentAppResp := false
	for i := len(ms) - 1; i >= 0; i-- {
		if _, hit := s.store.GetMember(ms[i].To); !hit {
			ms[i].To = 0
		}
		if ms[i].Type == pb.MsgAppResp {
			if sentAppResp {
				ms[i].To = 0
			} else {
				sentAppResp = true
			}
		}

		if ms[i].Type == pb.MsgSnap {
			select {
			case s.snapMsgc <- ms[i]:
			default:
				s.shotter.Delete(string(ms[i].Snapshot.Data))
				s.n.ReportSnapshot(ms[i].To, raft.SnapshotFailure)
			}
			ms[i].To = 0
		}
	}
	return ms
}

func (s *raftServer) run() {
	timeout := s.proposeTimeout
	for {
		select {
		case <-s.stopc:
			return
		case pr := <-s.propc:
			var nrs []notifier
			msg := pb.Message{Type: pb.MsgProp, Entries: []pb.Entry{{Type: pr.entryType, Data: pr.b}}}
			nrs = append(nrs, pr.nr)
			for i := 0; i < 64; i++ {
				var done bool
				select {
				case pr = <-s.propc:
					msg.Entries = append(msg.Entries, pb.Entry{Type: pr.entryType, Data: pr.b})
					nrs = append(nrs, pr.nr)
				default:
					done = true
				}
				if done {
					break
				}
			}
			ctx, cancel := context.WithTimeout(context.Background(), timeout)
			// raft的step方法是基于给定的msg信息去推进状态机
			if err := s.n.Step(ctx, msg); err != nil {
				for _, nr := range nrs {
					nr.notify(err) // Step出错，通过notify提醒Propose调用方

				}
			}
			cancel()
		}
	}
}

func (s *raftServer) linearizableReadLoop() {
	var rs raft.ReadState
	timeout := s.proposeTimeout

	for {
		readId := strconv.AppendUint([]byte{}, s.idGen.Next(), 10)
		select {
		case <-s.readwaitc:
		case <-s.stopc:
			return
		}

		nextnr := newReadIndexNotifier()
		nr := s.readNotifier.Load().(*readIndexNotifier)

		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		err := s.n.ReadIndex(ctx, readId)
		if err != nil {
			cancel()
			nr.Notify(err)
			s.readNotifier.Store(nextnr)
			continue
		}

		var (
			done      bool
			isTimeout bool
		)

		for !done {
			select {
			case rs = <-s.readStateC:
				done = bytes.Equal(rs.RequestCtx, readId)
				if !done {
					log.Warn("ignored out-of-date read index response")
				}
			case <-ctx.Done():
				done = true
				isTimeout = true
				nr.Notify(ErrTimeout)
				log.Warnf("the length of applyC is %d", len(s.applyc))
			case <-s.stopc:
				cancel()
				nr.Notify(ErrStopped)
				s.readNotifier.Store(nextnr)
				return
			}
		}
		cancel()
		if isTimeout {
			s.readNotifier.Store(nextnr)
			continue
		}

		if s.store.Applied() < rs.Index {
			select {
			case <-s.applyWait.Wait(rs.Index):
			case <-s.stopc:
				nr.Notify(ErrStopped)
				s.readNotifier.Store(nextnr)
				return
			}
		}

		nr.Notify(nil)
		s.readNotifier.Store(nextnr)
	}
}

func (s *raftServer) addMember(member Member) {
	s.store.AddMembers(member)
	if member.NodeID != s.cfg.NodeId {
		s.tr.AddMember(member)
	}
}

func (s *raftServer) removeMember(id uint64) {
	s.store.RemoveMember(id)
	s.tr.RemoveMember(id)
}

func (s *raftServer) updateMembers(mbs []*Member) {
	s.store.SetMembers(mbs)
	s.tr.SetMembers(mbs)
}

func (s *raftServer) handleMessage(msgs raftMsgs) error {
	ctx, cancel := context.WithTimeout(context.Background(), s.proposeTimeout)
	defer cancel()
	for i := 0; i < msgs.Len(); i++ {
		if err := s.n.Step(ctx, msgs[i]); err != nil {
			return err
		}
	}
	return nil
}

func (s *raftServer) handleSnapshot(st Snapshot) error {
	select {
	case s.snapshotC <- st:
	case <-s.stopc:
		return ErrStopped
	}

	return st.(*applySnapshot).nr.wait(context.TODO(), s.stopc)
}
