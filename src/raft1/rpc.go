package raft

import (
	// "time"
	"time"

	"6.5840/raftapi"
)

// AppendEntriesArgs defines the arguments for the AppendEntries RPC.
type AppendEntriesArgs struct {
	Term     int
	LeaderId int
	// Add other fields as necessary
	PrevLogIndex int        // Leader 的日志中，紧挨着新条目之前的索引
	PrevLogTerm  int        // PrevLogIndex 处日志条目的任期
	Entries      []LogEntry // 需要复制的日志条目，可以为空（用于心跳）
	LeaderCommit int        // Leader 的 commitIndex，告诉 Follower 最小需要提交的日志索引
}

// AppendEntriesReply defines the reply for the AppendEntries RPC.
type AppendEntriesReply struct {
	// Add fields as necessary
	Term          int  // Follower 的当前任期
	Success       bool // 是否成功接收心跳
	ConflictIndex int  // Follower 期望的 nextIndex
	ConflictTerm  int  // Follower 发现冲突的 Term
}
// sendAppendEntries sends an AppendEntries RPC to a specific server.
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	return rf.peers[server].Call("Raft.AppendEntries", args, reply)
}
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	needApply := false

	// 如果 Leader 过时，拒绝 RPC。
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		rf.mu.Unlock()
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.persist()
	}
	rf.state = Follower
	rf.electionReset = time.Now()
	rf.electionTimeout = randomElectionTimeout()

	// PrevLogIndex 在快照之前，告诉 leader 从快照之后继续。
	if args.PrevLogIndex < rf.lastIncludedIndex {
		reply.Term = rf.currentTerm
		reply.Success = false
		reply.ConflictIndex = rf.lastIncludedIndex + 1
		reply.ConflictTerm = -1
		rf.mu.Unlock()
		return
	}

	lastLogIndex := rf.getLastLogIndex()
	if args.PrevLogIndex > lastLogIndex {
		reply.Term = rf.currentTerm
		reply.Success = false
		reply.ConflictIndex = lastLogIndex + 1
		reply.ConflictTerm = -1
		rf.mu.Unlock()
		return
	}

	// PrevLog term 不匹配，返回冲突信息用于快速回退。
	if rf.getLogTerm(args.PrevLogIndex) != args.PrevLogTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		conflictTerm := rf.getLogTerm(args.PrevLogIndex)
		reply.ConflictTerm = conflictTerm

		firstIndex := args.PrevLogIndex
		for i := args.PrevLogIndex - 1; i >= rf.lastIncludedIndex; i-- {
			if rf.getLogTerm(i) != conflictTerm {
				firstIndex = i + 1
				break
			}
		}
		reply.ConflictIndex = firstIndex
		rf.mu.Unlock()
		return
	}

	// 追加日志（处理冲突覆盖）。
	insertAt := args.PrevLogIndex + 1
	for i, entry := range args.Entries {
		index := insertAt + i
		if index <= rf.getLastLogIndex() {
			if rf.getLogTerm(index) != entry.Term {
				relative := index - rf.lastIncludedIndex
				rf.log = rf.log[:relative]
				rf.log = append(rf.log, args.Entries[i:]...)
				rf.persist()
				break
			}
			continue
		}
		rf.log = append(rf.log, args.Entries[i:]...)
		rf.persist()
		break
	}

	// 根据 leaderCommit 推进 commitIndex。
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.getLastLogIndex())
		needApply = true
	}

	reply.Term = rf.currentTerm
	reply.Success = true
	if needApply {
		rf.notifyApplyLocked()
	}
	rf.mu.Unlock()
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	return rf.peers[server].Call("Raft.RequestVote", args, reply)
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		rf.mu.Unlock()
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.persist()
	}
	rf.state = Follower
	rf.electionReset = time.Now()
	rf.electionTimeout = randomElectionTimeout()

	if args.LastIncludedIndex <= rf.lastIncludedIndex {
		reply.Term = rf.currentTerm
		rf.mu.Unlock()
		return
	}

	lastIncludedTerm := args.LastIncludedTerm
	lastIncludedIndex := args.LastIncludedIndex
	snapshotData := args.Data
	rf.mu.Unlock()

	ok := true
	if rf.condInstallSnapshotFn != nil {
		ok = rf.condInstallSnapshotFn(lastIncludedTerm, lastIncludedIndex, snapshotData)
	}
	if !ok {
		return
	}

	rf.mu.Lock()
	if lastIncludedIndex <= rf.lastIncludedIndex {
		reply.Term = rf.currentTerm
		rf.mu.Unlock()
		return
	}
	// State machine already has newer data; ignore stale snapshot delivery.
	if lastIncludedIndex <= rf.lastApplied {
		reply.Term = rf.currentTerm
		rf.mu.Unlock()
		return
	}

	// Keep suffix only if local log contains snapshot boundary entry with same term.
	newLog := []LogEntry{{Term: lastIncludedTerm}}
	if lastIncludedIndex <= rf.getLastLogIndex() &&
		rf.getLogTerm(lastIncludedIndex) == lastIncludedTerm {
		start := lastIncludedIndex - rf.lastIncludedIndex + 1
		if start < len(rf.log) {
			newLog = append(newLog, rf.log[start:]...)
		}
	}

	rf.lastIncludedIndex = lastIncludedIndex
	rf.lastIncludedTerm = lastIncludedTerm
	rf.log = newLog
	rf.snapshot = snapshotData
	if rf.commitIndex < lastIncludedIndex {
		rf.commitIndex = lastIncludedIndex
	}
	rf.persister.Save(rf.encodeState(), rf.snapshot)
	reply.Term = rf.currentTerm

	applyMsg := raftapi.ApplyMsg{
		CommandValid:  false,
		SnapshotValid: true,
		Snapshot:      snapshotData,
		SnapshotTerm:  lastIncludedTerm,
		SnapshotIndex: lastIncludedIndex,
	}
	rf.pendingSnapshot = &applyMsg
	rf.notifyApplyLocked()
	rf.mu.Unlock()
}