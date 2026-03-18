package raft

import (
	"math/rand"
	"sync/atomic"
	"time"
)

func randomElectionTimeout() time.Duration {
	return time.Duration(450+rand.Intn(300)) * time.Millisecond
}

func (rf *Raft) startElection() {
	// Must be called with rf.mu held.
	rf.state = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.electionReset = time.Now()
	rf.electionTimeout = randomElectionTimeout()
	rf.persist()

	term := rf.currentTerm
	lastLogIndex := rf.getLastLogIndex()
	lastLogTerm := rf.getLastLogTerm()
	voteCount := int32(1)

	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		server := i
		go func() {
			args := RequestVoteArgs{
				Term:         term,
				CandidateId:  rf.me,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			}
			reply := RequestVoteReply{}
			if !rf.sendRequestVote(server, &args, &reply) {
				return
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()

			// Stale reply or not a candidate anymore.
			if rf.state != Candidate || rf.currentTerm != term {
				return
			}

			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.votedFor = -1
				rf.state = Follower
				rf.electionReset = time.Now()
				rf.electionTimeout = randomElectionTimeout()
				rf.persist()
				return
			}

			if !reply.VoteGranted {
				return
			}

			votes := atomic.AddInt32(&voteCount, 1)
			if votes > int32(len(rf.peers)/2) && rf.state == Candidate {
				rf.state = Leader
				last := rf.getLastLogIndex()
				for i := range rf.peers {
					rf.nextIndex[i] = last + 1
					rf.matchIndex[i] = 0
				}
				rf.matchIndex[rf.me] = last
				go rf.broadcastAppendEntries()
			}
		}()
	}
}

func (rf *Raft) leaderTicker() {
	for !rf.killed() {
		rf.mu.Lock()
		isLeader := rf.state == Leader
		rf.mu.Unlock()
		if isLeader {
			rf.broadcastAppendEntries()
		}
		time.Sleep(100 * time.Millisecond)
	}
}
