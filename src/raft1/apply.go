package raft

import (
	"fmt"

	"6.5840/raftapi"
)

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

func (rf *Raft) applyLogs() {
	// 确保从 lastIncludedIndex 开始应用日志
	// startIndex := rf.lastIncludedIndex + 1

	// 从 lastApplied 开始，确保没有越界
	for rf.lastApplied < rf.commitIndex {
		rf.lastApplied++
		// if rf.lastApplied < startIndex {
		//     continue  // 跳过已经被快照包含的日志
		// }

		// 防止超出日志范围
		// if rf.lastApplied - rf.lastIncludedIndex  >= len(rf.log) {
		//     break // 如果超出日志范围，停止应用
		// }

		relativeIndex, _ := rf.getLogIndex(rf.lastApplied)
		msg := raftapi.ApplyMsg{
			CommandValid: true,
			// Command:      rf.log[rf.lastApplied - rf.lastIncludedIndex - 1].Command,
			Command: rf.log[relativeIndex].Command,
			// Command:      rf.log[rf.lastApplied].Command,
			CommandIndex: rf.lastApplied,
		}

		rf.applyCh <- msg
	}
}
func (rf *Raft) updateCommitIndex() {
	fmt.Printf("🔍 [Leader %d Term %d] Begin updateCommitIndex: commitIndex=%d, lastLogIndex=%d, matchIndex=%v\n", rf.me, rf.currentTerm, rf.commitIndex, rf.getLastLogIndex(), rf.matchIndex)
	for i := rf.getLastLogIndex(); i > rf.commitIndex; i-- {
		// if i <= rf.lastIncludedIndex {
		// 	continue
		// }
		count := 1 // 统计复制日志的节点数
		for j := range rf.peers {
			if j != rf.me && rf.matchIndex[j] >= i {
				count++
			}
		}
		// fmt.Printf("[Leader %d Term %d] Try commit i=%d, matchCount=%d, matchIndex=%v, myTerm=%d, logTerm=%d\n",
		// 	rf.me, rf.currentTerm, i, count, rf.matchIndex, rf.currentTerm, rf.getLogTerm(i))
		fmt.Printf("🧩 Check i=%d: matchCount=%d, currentTerm=%d\n", i, count,  rf.currentTerm)
		if count > len(rf.peers)/2 && rf.getLogTerm(i) == rf.currentTerm {
			fmt.Printf("✅ Ready to commit index=%d\n", i)
			rf.commitIndex = i
			fmt.Printf("✅ [Leader %d Term %d] Commit index advanced to %d, logTerm=%d, matchCount=%d\n", rf.me, rf.currentTerm, i, rf.getLogTerm(i), count)
			rf.applyLogs()
			return
		}
	}
}

func (rf *Raft) applyPendingLogs() {
	go func() {
		rf.mu.Lock()
		applied := rf.lastApplied
		committed := rf.commitIndex
		log := rf.log // 不用复制，直接引用
		rf.mu.Unlock()

		// 循环通过 commitIndex 应用已提交的日志
		for i := applied + 1; i <= committed; i++ {
			rf.mu.Lock()
			rf.lastApplied = i
			rf.mu.Unlock()

			rf.applyCh <- raftapi.ApplyMsg{
				CommandValid: true,
				Command:      log[i].Command,
				CommandIndex: i,
			}
		}
	}()
}
