package raft

import (
	"fmt"
	"log"
)


type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

	rf.mu.Lock()
	fmt.Printf("📸 [Leader %d] Snapshot called at index=%d, log length=%d\n", rf.me, index, len(rf.log))
	fmt.Printf("👋%d\n", rf.lastIncludedIndex)
	if index <= rf.lastIncludedIndex {
		return // 快照落后，忽略
	}

	// 计算相对 index
	offset := index - rf.lastIncludedIndex
	if offset > len(rf.log) {
		log.Fatalf("Snapshot offset out of range: offset=%d logLen=%d", offset, len(rf.log))
	}

	rf.lastIncludedTerm = rf.log[offset-1].Term
	rf.lastIncludedIndex = index

	// 截断日志：只保留从 index 开始的日志
	rf.log = append([]LogEntry{{Term: rf.lastIncludedTerm}}, rf.log[offset:]...)

	rf.snapshot = snapshot

	// 更新状态
	rf.commitIndex = max(rf.commitIndex, index)
	rf.lastApplied = max(rf.lastApplied, index)

	// 持久化
	rf.snapshot = snapshot
	rf.mu.Unlock()
	rf.persister.Save(rf.encodeState(), rf.snapshot)
	rf.mu.Lock()
	fmt.Printf("✅ SnapshotDone Node %d: lastIncludedIndex=%d, lastIncludedTerm=%d, log=%v\n",
	rf.me, rf.lastIncludedIndex, rf.lastIncludedTerm, rf.log)
}

func (rf *Raft) readSnapshot(snapshot []byte) {
	if snapshot == nil || len(snapshot) == 0 {
		return
	}
	rf.snapshot = snapshot
}

func (rf *Raft) sendInstallSnapshot(server int) {
	rf.mu.Lock()
	// defer rf.mu.Unlock()
	args := InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.lastIncludedIndex,
		LastIncludedTerm:  rf.lastIncludedTerm,
		Data:              rf.persister.ReadSnapshot(),
	}
	rf.mu.Unlock()

	var reply InstallSnapshotReply
	if rf.sendInstallSnapshotRPC(server, &args, &reply) {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.state = Follower
			rf.votedFor = -1
			rf.persist()
			return
		}
		rf.nextIndex[server] = max(rf.nextIndex[server], args.LastIncludedIndex+1)
		rf.matchIndex[server] = max(rf.matchIndex[server], args.LastIncludedIndex)
	}
}
func (rf *Raft) sendInstallSnapshotRPC(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	return rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
}
func (rf *Raft) SetCondInstallSnapshotCallback(cb func(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool) {
	rf.condInstallSnapshotFn = cb
}