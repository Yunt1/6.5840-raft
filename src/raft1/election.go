package raft


import (
	"math/rand"
	"time"
	"fmt"
	"sync/atomic"
)



func (rf *Raft) startElection() {
	// Debug: Log current term and state
	fmt.Printf("[Candidate %d] Start election, current term: %d, current state: %v\n", rf.me, rf.currentTerm, rf.state)

	// 检查是否是 Leader，避免死锁
	rf.state = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	fmt.Printf("🗳️ [Candidate %d] start election, term=%d\n", rf.me, rf.currentTerm)
	rf.persist()

	voteCount := int32(1) // 给自己投票

	// 启动 goroutine 发送 RequestVote 请求
	for i := range rf.peers {
		if i != rf.me {
			server := i
			go func(server int) {
				// 打印 Node 当前状态
				fmt.Printf("[Candidate %d] Node %d current state: %v\n", rf.me, server, rf.state)
				
				fmt.Printf("Sending RequestVote to %d from Candidate %d\n", server, rf.me)
				args := RequestVoteArgs{
					Term:         rf.currentTerm,
					CandidateId:  rf.me,
					LastLogIndex: rf.getLastLogIndex(),
					LastLogTerm:  rf.getLastLogTerm(),
				}
				reply := RequestVoteReply{}

				// Debug: Log request parameters
				fmt.Printf("[Candidate %d] Sending RequestVote to Node %d (Term=%d, CandidateId=%d, LastLogIndex=%d, LastLogTerm=%d)\n", rf.me, server, args.Term, args.CandidateId, args.LastLogIndex, args.LastLogTerm)

				// 调试：发送请求前记录状态
				if rf.sendRequestVote(server, &args, &reply) {
					// 获取锁进行状态修改
					rf.mu.Lock()
					// Debug: Log response from RequestVote
					fmt.Printf("[Candidate %d] received RequestVoteReply from %d: term=%d, vote=%v\n", rf.me, server, reply.Term, reply.VoteGranted)
					defer rf.mu.Unlock()

					if reply.Term > rf.currentTerm {
						// 更高任期，降级为 Follower
						fmt.Printf("[Candidate %d] found higher term %d, back to Follower\n", rf.me, reply.Term)
						rf.currentTerm = reply.Term
						rf.votedFor = -1
						rf.persist()
						rf.state = Follower
						return
					}

					if reply.VoteGranted {
						fmt.Printf("[Candidate %d] Node %d granted vote\n", rf.me, server)
						if rf.state == Candidate {
							atomic.AddInt32(&voteCount, 1)
							votes := atomic.LoadInt32(&voteCount)

							// 调试：打印当前投票数
							fmt.Printf("[Candidate %d] Current vote count=%d, total peers=%d\n", rf.me, votes, len(rf.peers))

							if votes > int32(len(rf.peers)/2) {
								// 获得多数票，成为 Leader
								rf.state = Leader
								for i := range rf.peers {
									rf.nextIndex[i] = rf.getLastLogIndex() + 1
									rf.matchIndex[i] = 0
								}
								rf.matchIndex[rf.me] = rf.getLastLogIndex()
								go rf.broadcastAppendEntries()
							}
						}
					} else {
						// 如果 Node 拒绝投票请求
						fmt.Printf("[Candidate %d] Node %d rejected vote\n", rf.me, server)
					}
				} else {
					// 如果请求投票失败，打印详细信息
					fmt.Printf("[Candidate %d] failed to send RequestVote to %d\n", rf.me, server)
					fmt.Printf("[Candidate %d] Node %d current state: %v\n", rf.me, server, rf.state)
				}
			}(server)
		}
	}

	// 超时处理，如果超时还没有成为Leader，重新发起选举
	time.AfterFunc(time.Duration(300+rand.Intn(300))*time.Millisecond, func() {
		rf.mu.Lock()
		if rf.state == Candidate {
			// Debug: Log timeout details
			fmt.Printf("[Candidate %d] Timeout, starting new election. Current state before new election: %v\n", rf.me, rf.state)
			rf.startElection()
		}
		rf.mu.Unlock()
	})
}

func (rf *Raft) leaderTicker() {
	for !rf.killed() {
		if rf.state == Leader {
			rf.broadcastAppendEntries()
		}
		time.Sleep(50 * time.Millisecond) // 发送心跳
	}
}
