package raft

import (
	"fmt"
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
	
	// fmt.Printf(("Node %d:term=%d, log=%v, commitIndex=%d\n"), rf.me, rf.currentTerm, rf.log, rf.commitIndex)
	// fmt.Printf("[Follower %d] ✅ Received AppendEntries: term=%d, log now=%v, commitIndex=%d\n", rf.me, rf.currentTerm, rf.log, rf.commitIndex)
	defer rf.mu.Unlock()
	
	// 如果 Leader 过时，拒绝心跳
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}
	if args.Term >= rf.currentTerm {
		// fmt.Printf("[Follower %d] updating term from %d to %d due to AppendEntries\n", rf.me, rf.currentTerm, args.Term)
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.persist()
		rf.state = Follower
	}
		// 如果有日志条目（非心跳），标记为已接收到心跳
	if len(args.Entries) >= 0 {
		rf.heartbeatReceived = true
	}

	// fmt.Printf("args.PrevLogIndex=%d,rf.lastIncludedIndex=%d\n,rf.log[args.PrevLogIndex - rf.lastIncludedIndex].Term=%d,args.PrevLogTerm=%d\n",args.PrevLogIndex,rf.lastIncludedIndex,rf.log[args.PrevLogIndex - rf.lastIncludedIndex].Term,args.PrevLogTerm,)
	if args.PrevLogIndex < rf.lastIncludedIndex {
		reply.Term = rf.currentTerm
		reply.Success = false
		reply.ConflictIndex = rf.lastIncludedIndex + 1
		reply.ConflictTerm = -1
		return
	}

	if args.PrevLogIndex-rf.lastIncludedIndex >= len(rf.log) {
		// 缺失日志
		reply.Term = rf.currentTerm
		reply.Success = false
		reply.ConflictIndex = rf.lastIncludedIndex + len(rf.log)
		reply.ConflictTerm = -1
		return
	}

	// 检查 term 是否匹配
	if rf.log[args.PrevLogIndex-rf.lastIncludedIndex].Term != args.PrevLogTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		conflictTerm := rf.getLogTerm(args.PrevLogIndex)
		reply.ConflictTerm = conflictTerm

		// 找到该 term 第一个出现的位置（global index）
		firstIndex := rf.lastIncludedIndex
		for i := 0; i < len(rf.log); i++ {
			if rf.log[i].Term == conflictTerm {
				firstIndex = rf.lastIncludedIndex + i
				break
			}
		}
		reply.ConflictIndex = firstIndex
		return
	}
	// 4. 日志匹配成功，开始追加新日志
	// i：本地日志中应开始替换或追加的索引
	i := args.PrevLogIndex + 1
	j := 0
	// 检查 Leader 发来的每个日志条目，找到冲突的地方
	for ; j < len(args.Entries); j++ {
		// 如果本地日志已经没有对应条目，则退出循环，直接追加新日志
		if i+j >= len(rf.log) {
			break
		}
		// 如果存在条目且条目的任期不匹配，则发现冲突
		relativeIndex := i + j - rf.lastIncludedIndex
		if relativeIndex >= len(rf.log) {
			break
		}
		if rf.log[relativeIndex].Term != args.Entries[j].Term {
			// 冲突处理
			rf.log = rf.log[:relativeIndex]
			rf.persist()
			break
		}
	}
	// 追加 Leader 发送过来的新日志条目中剩余部分

	if j < len(args.Entries) {
		rf.log = append(rf.log, args.Entries[j:]...)
		rf.persist()
		// fmt.Printf("[Follower %d] Appending logs at index=%d, entries=%v\n", rf.me, i+j, args.Entries[j:])
	}

	// 5. 根据 LeaderCommit 更新本地 commitIndex
	if args.LeaderCommit > rf.commitIndex {
		var needApply bool
		if args.LeaderCommit > rf.commitIndex {
			rf.commitIndex = min(args.LeaderCommit, rf.lastIncludedIndex+len(rf.log)-1)
			needApply = true
		}

		reply.Term = rf.currentTerm
		reply.Success = true

		if needApply {
			rf.applyLogs()
		}
		return
	}
	reply.Term = rf.currentTerm
	reply.Success = true

}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	fmt.Printf("→ [Node %d][state]=%d sending RequestVote to %d\n", rf.me, rf.state, server)
	timeout := time.After(time.Second * 5)  // 5 seconds timeout
	done := make(chan bool)
	
	// Send the RPC request in a goroutine
	go func() {
		ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
		done <- ok
	}()
	
	// Wait for the response or timeout
	select {
	case ok := <-done:
		fmt.Printf("← [Node %d] sendRequestVote to %d success=%v\n", rf.me, server, ok)
		return ok
	case <-timeout:
		fmt.Printf("Timeout when sending RequestVote to %d\n", server)
		return false
	}
}
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	// defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = Follower
		rf.votedFor = -1
		rf.persist()
	}

	if args.LastIncludedIndex <= rf.lastIncludedIndex {
		return
	}

	// 释放锁，调用上层判断是否要接受 snapshot
	rf.mu.Unlock()
	ok := true
	if rf.condInstallSnapshotFn != nil {
		ok = rf.condInstallSnapshotFn(args.LastIncludedTerm, args.LastIncludedIndex, args.Data)
	}
	if !ok {
		return
	}
	rf.mu.Lock() // 重新加锁

	// 保存旧 index 计算 offset
	oldIndex := rf.lastIncludedIndex
	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.lastIncludedTerm = args.LastIncludedTerm

	if args.LastIncludedIndex < rf.getLastLogIndex() {
		offset := args.LastIncludedIndex - oldIndex
		rf.log = append([]LogEntry{{Term: args.LastIncludedTerm}}, rf.log[offset:]...)
	} else {
		rf.log = []LogEntry{{Term: args.LastIncludedTerm}} // dummy log
	}

	rf.commitIndex = max(rf.commitIndex, rf.lastIncludedIndex)
	rf.lastApplied = max(rf.lastApplied, rf.lastIncludedIndex)
	rf.snapshot = args.Data
	rf.mu.Unlock()
	rf.persister.Save(rf.encodeState(), rf.snapshot)
	rf.mu.Lock()

	// 💡 通知上层服务使用 snapshot
	rf.applyCh <- raftapi.ApplyMsg{
		CommandValid:  false,
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  args.LastIncludedTerm,
		SnapshotIndex: args.LastIncludedIndex,
	}
}