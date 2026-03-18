package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	//	"bytes"
	// "fmt"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	// "6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

type State int

const (
	Follower State = iota
	Candidate
	Leader
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	state       State      // 当前节点的状态
	currentTerm int        // 当前任期号
	votedFor    int        // 在当前任期中投票给哪个候选者，-1 表示未投票
	log         []LogEntry // 日志条目列表，log[0] 可作为占位项
	// 易失性状态：所有服务器上的状态
	commitIndex int // 已提交的最大日志索引
	lastApplied int // 最后被应用到状态机的日志索引

	// 易失性状态（仅 Leader 保存）：用于日志复制
	nextIndex  []int // 对于每个 follower，下一个要发送的日志索引
	matchIndex []int // 对于每个 follower，已知复制到的最高日志索引

	// 用于向服务层（或测试器）发送已提交日志条目
	applyCh           chan raftapi.ApplyMsg
	lastIncludedIndex int    // 快照中包含的最后日志索引
	lastIncludedTerm  int    // 快照中包含的最后日志条目的任期
	snapshot          []byte // 快照数据
	// Lab 3D: for snapshot decision
	condInstallSnapshotFn func(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool
	electionReset         time.Time
	electionTimeout       time.Duration
	applyCond             *sync.Cond
	pendingSnapshot       *raftapi.ApplyMsg
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// var term int
	// var isleader bool
	// Your code here (3A).
	return rf.currentTerm, (rf.state == Leader)
}


func (rf *Raft) Start(command interface{}) (int, int, bool) {
	// fmt.Printf("🚀 [Leader %d] Start appending command, term=%d\n", rf.me, rf.currentTerm)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 若不是 Leader，无法发起新的日志复制
	if rf.state != Leader {
		return -1, rf.currentTerm, false
	}

	// 1. 创建新日志条目并追加到日志
	newEntry := LogEntry{
		Term:    rf.currentTerm,
		Command: command,
	}
	rf.log = append(rf.log, newEntry)
	// fmt.Printf("📜 [Leader %d] Appending new log entry: term=%d, command=%v\n", rf.me, rf.currentTerm, command)
	rf.persist()
	index := rf.getLastLogIndex() // 该条目所在的日志索引

	// 2. 异步将日志复制到其他节点
	go rf.broadcastAppendEntries()

	// 3. 立即返回，而不等待复制完成
	return index, rf.currentTerm, true
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	// fmt.Printf("💀 Node %d is being killed,state=%d\n", rf.me,rf.state)
	atomic.StoreInt32(&rf.dead, 1)
	rf.mu.Lock()
	if rf.applyCond != nil {
		rf.applyCond.Broadcast()
	}
	rf.mu.Unlock()
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyCh = applyCh

	// Your initialization code here (3A, 3B, 3C).
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.state = Follower
	rf.lastIncludedIndex = 0
	rf.lastIncludedTerm = 0
	rf.snapshot = nil
	rf.log = []LogEntry{{Term: 0}} 
	rf.electionTimeout = randomElectionTimeout()
	rf.electionReset = time.Now()
	// Initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.readSnapshot(persister.ReadSnapshot())
	if rf.commitIndex < rf.lastIncludedIndex {
		rf.commitIndex = rf.lastIncludedIndex
	}
	if rf.lastApplied < rf.lastIncludedIndex {
		rf.lastApplied = rf.lastIncludedIndex
	}
	rf.matchIndex = make([]int, len(peers))
	rf.nextIndex = make([]int, len(peers))
	rf.applyCond = sync.NewCond(&rf.mu)

	// If only one node, skip ticker and leaderTicker goroutines
	// Start ticker goroutine to start elections
	go rf.ticker()
	go rf.leaderTicker()
	go rf.applier()

	// return raft instance
	return rf
}



