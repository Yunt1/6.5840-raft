package raft

import (
	"bytes"
	"fmt"
	"log"

	"6.5840/labgob"
)
// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}


func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	// 编码持久化字段
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	fmt.Printf("Node %d persisting: term=%d,votedFor=%d,log=%v,state=%d\n", rf.me, rf.currentTerm, rf.votedFor, rf.log, rf.state)
	// 写入 Persister（第二个参数先设为 nil）
	rf.persister.Save(w.Bytes(), rf.snapshot)
	// After persist
	// fmt.Printf(
	//     "📦 [Persist-After ] Node %d | term=%d | votedFor=%d ,state=%d\n",
	//     rf.me, rf.currentTerm, rf.votedFor,rf.state,
	// )

}
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		rf.currentTerm = 0
		rf.votedFor = -1
		rf.log = make([]LogEntry, 1) // 初始化 dummy entry
		rf.lastIncludedIndex = 0
		rf.lastIncludedTerm = 0
		return
	}
	// Your code here (3C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
	// if data == nil || len(data) == 0 {
	//     return // 初始为空，跳过
	// }

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	// 按照 persist 时的顺序解码
	var currentTerm int
	var votedFor int
	var savedLog []LogEntry
	var lastIncludedIndex, lastIncludedTerm int
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&savedLog) != nil ||
		d.Decode(&lastIncludedIndex) != nil ||
		d.Decode(&lastIncludedTerm) != nil {
		// 出错了：可能数据损坏
		log.Fatalf("readPersist: decoding failed")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = savedLog
		rf.lastIncludedIndex = lastIncludedIndex
		rf.lastIncludedTerm = lastIncludedTerm
		// fmt.Printf("Node%d reading: term=%d, votedFor=%d, log=%v\n",rf.me, rf.currentTerm, rf.votedFor, rf.log)
	}
}

func (rf *Raft) encodeState() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)

	return w.Bytes()
}
