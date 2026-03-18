package raft

import (
	// "fmt"
	"time"
)

func (rf *Raft) ticker() {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.state != Leader && time.Since(rf.electionReset) >= rf.electionTimeout {
			rf.startElection()
		}
		rf.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
	}
}

// func (rf *Raft) receiveHeartbeat() {
//     rf.mu.Lock()
//     rf.heartbeatReceived = true  // 收到心跳，标记为 true
//     rf.mu.Unlock()
// }
func (rf *Raft) broadcastHeartbeat() {
	for i := range rf.peers {
		if i != rf.me {
			server := i
			go func(server int) {
				rf.mu.Lock()
				args := AppendEntriesArgs{
					Term:         rf.currentTerm,
					LeaderId:     rf.me,
					Entries:      []LogEntry{}, // 不携带日志条目，作为心跳
					LeaderCommit: rf.commitIndex,
				}
				rf.mu.Unlock()

				var reply AppendEntriesReply
				rf.sendAppendEntries(server, &args, &reply)
			}(server)
		}
	}
}
func (rf *Raft) broadcastAppendEntries() {

	for i := range rf.peers {
		if i != rf.me {
			server := i
			// fmt.Printf("不知道现在锁没锁\n")
			go func(server int) {
				rf.mu.Lock() // 锁定
				// fmt.Printf("📤 [Leader %d Term %d] Sending AppendEntries to Node %d | nextIndex=%d | lastLogIndex=%d\n",
					// rf.me, rf.currentTerm, server, rf.nextIndex[server], rf.getLastLogIndex())
				if rf.state != Leader {
					rf.mu.Unlock() // 确保在释放锁之前有对应的锁
					return
				}

				if rf.nextIndex[server] <= rf.lastIncludedIndex {
					rf.mu.Unlock() // 提前释放锁，避免阻塞
					rf.sendInstallSnapshot(server)
					return
				}
				prevLogIndex := rf.nextIndex[server] - 1
				prevLogTerm := rf.getLogTerm(prevLogIndex)
				entries := rf.getLogSliceFrom(rf.nextIndex[server])
				if entries == nil {
					rf.mu.Unlock() // 确保释放锁
					return
				}
				entriesCopy := append([]LogEntry(nil), entries...)

				args := AppendEntriesArgs{
					Term:         rf.currentTerm,
					LeaderId:     rf.me,
					PrevLogIndex: prevLogIndex,
					PrevLogTerm:  prevLogTerm,
					Entries:      entriesCopy,
					LeaderCommit: rf.commitIndex,
				}
				rf.mu.Unlock() // 解锁，避免阻塞

				var reply AppendEntriesReply
				ok := rf.sendAppendEntries(server, &args, &reply)
				if !ok {
					return
				}

				rf.mu.Lock()         // 再次获取锁
				defer rf.mu.Unlock() // 使用 defer 保证解锁
				if rf.state != Leader || rf.currentTerm != args.Term {
					return
				}

				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.state = Follower
					rf.votedFor = -1
					rf.persist()
					return
				}

				if reply.Success {
					rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
					rf.nextIndex[server] = rf.matchIndex[server] + 1
					if rf.updateCommitIndex() {
						rf.notifyApplyLocked()
					}
					// fmt.Printf("✅ [Leader %d Term %d] AppendEntries to Node %d succeeded | matchIndex=%d | nextIndex=%d\n",
						// rf.me, rf.currentTerm, server, rf.matchIndex[server], rf.nextIndex[server])
				} else {
					// fmt.Printf("❌ [Leader %d Term %d] AppendEntries to Node %d failed | ConflictTerm=%d | ConflictIndex=%d | nextIndex(before)=%d\n",
						// rf.me, rf.currentTerm, server, reply.ConflictTerm, reply.ConflictIndex, rf.nextIndex[server])
					if reply.ConflictTerm != -1 {
						conflictIndex := -1
						for i := args.PrevLogIndex; i >= rf.lastIncludedIndex; i-- {
							if rf.getLogTerm(i) == reply.ConflictTerm {
								conflictIndex = i
								break
							}
						}
						if conflictIndex != -1 {
							rf.nextIndex[server] = conflictIndex + 1
						} else {
							rf.nextIndex[server] = reply.ConflictIndex
						}
					} else {
						rf.nextIndex[server] = reply.ConflictIndex
					}
					rf.nextIndex[server] = max(1, rf.nextIndex[server])
					// fmt.Printf("🔁 [Leader %d Term %d] Updated nextIndex for Node %d to %d after conflict\n",
					// 	rf.me, rf.currentTerm, server, rf.nextIndex[server])
				}
			}(server)
		}
	}
}