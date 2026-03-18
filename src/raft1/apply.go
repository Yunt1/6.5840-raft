package raft

import (
	"6.5840/raftapi"
)

func (rf *Raft) notifyApplyLocked() {
	if rf.applyCond != nil {
		rf.applyCond.Signal()
	}
}

func (rf *Raft) applier() {
	for !rf.killed() {
		rf.mu.Lock()
		for !rf.killed() && rf.pendingSnapshot == nil && rf.lastApplied >= rf.commitIndex {
			rf.applyCond.Wait()
		}
		if rf.killed() {
			rf.mu.Unlock()
			return
		}

		if rf.pendingSnapshot != nil {
			msg := *rf.pendingSnapshot
			rf.pendingSnapshot = nil
			if rf.lastApplied < msg.SnapshotIndex {
				rf.lastApplied = msg.SnapshotIndex
			}
			rf.mu.Unlock()
			rf.applyCh <- msg
			continue
		}

		rf.lastApplied++
		index := rf.lastApplied
		relativeIndex, err := rf.getLogIndex(index)
		if err != nil {
			rf.lastApplied--
			rf.mu.Unlock()
			continue
		}
		command := rf.log[relativeIndex].Command
		rf.mu.Unlock()
		rf.applyCh <- raftapi.ApplyMsg{
			CommandValid: true,
			Command:      command,
			CommandIndex: index,
		}
	}
}
func (rf *Raft) updateCommitIndex() bool {
	oldCommit := rf.commitIndex
	for i := rf.getLastLogIndex(); i > rf.commitIndex; i-- {
		count := 1 // 统计复制日志的节点数
		for j := range rf.peers {
			if j != rf.me && rf.matchIndex[j] >= i {
				count++
			}
		}
		if float64(count) > float64(len(rf.peers))/2.0 && rf.getLogTerm(i) == rf.currentTerm {
			rf.commitIndex = i
			break
		}
	}
	return rf.commitIndex > oldCommit
}
