package raft


import (
	"fmt"
	"log"
)
type LogEntry struct {
	Command interface{}
	Term    int
}
func (rf *Raft) getLogIndex(globalIndex int) (int, error) {
	if globalIndex < rf.lastIncludedIndex {
		return -1, fmt.Errorf("globalIndex=%d 已被 snapshot 包含", globalIndex)
	}
	relativeIndex := globalIndex - rf.lastIncludedIndex
	if relativeIndex >= len(rf.log) {
		return -1, fmt.Errorf("globalIndex=%d 超出日志范围，当前日志长度=%d", globalIndex, len(rf.log))
	}
	return relativeIndex, nil
}
func (rf *Raft) getLogTerm(globalIndex int) int {
	if globalIndex == rf.lastIncludedIndex {
		return rf.lastIncludedTerm
	}
	relativeIndex, err := rf.getLogIndex(globalIndex)
	if err != nil {
		log.Fatalf("getLogTerm: invalid globalIndex=%d: %v", globalIndex, err)
	}
	return rf.log[relativeIndex].Term
}
func (rf *Raft) getLog(globalIndex int) LogEntry {
	if globalIndex == rf.lastIncludedIndex {
		return LogEntry{Term: rf.lastIncludedTerm}
	}
	relativeIndex, err := rf.getLogIndex(globalIndex)
	if err != nil {
		log.Fatalf("[getLog] invalid globalIndex=%d: %v", globalIndex, err)
	}
	return rf.log[relativeIndex]
}
func (rf *Raft) getLastLogIndex() int {
	return rf.lastIncludedIndex + len(rf.log) - 1
}
func (rf *Raft) getLastLogTerm() int {
	return rf.getLogTerm(rf.getLastLogIndex())
}
func (rf *Raft) getLogSliceFrom(index int) []LogEntry {
	if index < rf.lastIncludedIndex {
		return nil
	}
	start := index - rf.lastIncludedIndex
	if start > len(rf.log) {
		return []LogEntry{} // 空切片表示没有日志
	}
	return rf.log[start:]
}