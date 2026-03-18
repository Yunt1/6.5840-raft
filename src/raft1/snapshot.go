package raft

import (
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
    rf.mu.Lock()
    defer rf.mu.Unlock() // 使用defer确保无论如何都会解锁

    if index <= rf.lastIncludedIndex {
        return // 快照落后，忽略
    }
    
    // 计算相对 index
    offset := index - rf.lastIncludedIndex
    if offset >= len(rf.log) {
        log.Fatalf("Snapshot offset out of range: offset=%d logLen=%d", offset, len(rf.log))
    }
    
    rf.lastIncludedTerm = rf.log[offset].Term
    rf.lastIncludedIndex = index
    
    // 截断日志：只保留从 index 开始的日志（不包括index）
    if offset+1 < len(rf.log) {
        rf.log = append([]LogEntry{{Term: rf.lastIncludedTerm}}, rf.log[offset+1:]...)
    } else {
        // 如果offset+1等于len(rf.log)，说明已经到日志末尾
        rf.log = []LogEntry{{Term: rf.lastIncludedTerm}}
    }
    
    rf.snapshot = snapshot

    // 持久化状态
    rf.persist()
    
}
func (rf *Raft) readSnapshot(snapshot []byte) {
	if snapshot == nil || len(snapshot) == 0 {
		return
	}
	rf.snapshot = snapshot
}

func (rf *Raft) sendInstallSnapshot(server int) {
	if rf.persister == nil || rf.peers == nil || server < 0 || server >= len(rf.peers) {
		return
	}
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
	if rf.peers == nil || server < 0 || server >= len(rf.peers) {
		return false
	}
	return rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
}
func (rf *Raft) SetCondInstallSnapshotCallback(cb func(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool) {
	rf.condInstallSnapshotFn = cb
}
