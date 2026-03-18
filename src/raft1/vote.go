package raft

// import "fmt"
import "time"

type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int // 候选人的任期
	CandidateId  int // 候选人的 ID
	LastLogIndex int // 候选人日志中最后一条日志的索引
	LastLogTerm  int // 候选人日志中最后一条日志的任期
}


type RequestVoteReply struct {
	// Your data here (3A).
	Term        int  // 接收者的当前任期（如果比请求者大，候选人应该更新）
	VoteGranted bool // 如果为 true，则投票给候选人
}


// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	// fmt.Printf("✅ [Node %d] ,term%d,log %v\n ", rf.me, rf.currentTerm, rf.log)
	// fmt.Printf("✅ [Node %d][state]=%d <- RequestVote from %d at term %d\n", rf.me,rf.state,args.CandidateId, args.Term,)
	// fmt.Printf("✌️\n")
	rf.mu.Lock()
	// fmt.Printf("✌️✌️\n")
	defer rf.mu.Unlock()
	// fmt.Printf("✌️✌️✌️\n")
	// fmt.Printf("[Node %d] received RequestVote: term=%d, candidate=%d\n", rf.me, args.Term, args.CandidateId)

	// 如果请求的任期比当前任期小，拒绝投票
	if args.Term < rf.currentTerm {
		// fmt.Printf("❌ [Node %d Term %d] Rejecting vote for %d at term %d (too old)\n", rf.me, rf.currentTerm, args.CandidateId, args.Term)
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	// 如果请求的任期比当前大，更新自己的任期，并转变为 Follower
	if args.Term > rf.currentTerm {
		// fmt.Printf("🔄 [Node %d Term %d] Updating term to %d\n", rf.me, rf.currentTerm, args.Term)
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.state = Follower
		rf.electionReset = time.Now()
		rf.electionTimeout = randomElectionTimeout()
		// fmt.Printf("🔄 [Node %d Term %d] Updating term to %d, votedFor=%d\n", rf.me, rf.currentTerm, args.Term, rf.votedFor)
		rf.persist()
	}
	// fmt.Printf("开始选票\n")
	// 判断是否可以投票
	voteGranted := false
	if args.Term == rf.currentTerm {
		if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && isUpToDate(args, rf) {
			rf.votedFor = args.CandidateId
			voteGranted = true
			rf.electionReset = time.Now()
			rf.electionTimeout = randomElectionTimeout()
			// fmt.Printf("✅ [Node %d Term %d] Voting for %d at term %d\n", rf.me, rf.currentTerm, args.CandidateId, args.Term)
			rf.persist()
		}
	}

	// 返回投票结果
	reply.Term = rf.currentTerm
	reply.VoteGranted = voteGranted
	// fmt.Printf("🗳️ [Node %d Term %d] votedFor=%d, candidate=%d, granted=%v, myLog=(%d,%d), candLog=(%d,%d)\n",
	// 	rf.me, rf.currentTerm, rf.votedFor, args.CandidateId, voteGranted,
	// 	rf.getLastLogTerm(), rf.getLastLogIndex(), args.LastLogTerm, args.LastLogIndex)
}

func isUpToDate(args *RequestVoteArgs, rf *Raft) bool {
	lastLogIndex := rf.getLastLogIndex()
	lastLogTerm := rf.getLastLogTerm()
	// 先比较日志的 term，再比较索引
	return (args.LastLogTerm > lastLogTerm) || (args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex)
}
