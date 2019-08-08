package raft

type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	// Your data here (2A).
	Term          int
	IsVoteGranted bool
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// 任期小，忽略
	if args.Term < rf.currentTerm {
		reply.IsVoteGranted = false
		reply.Term = rf.currentTerm

		return
	}
	// 任期大，无条件转为 Follower
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.role = Follower
		rf.voteFor = None
		reply.Term = rf.currentTerm
		return
	}

	upToDate := false

	if args.LastLogTerm > rf.lastTerm() {
		upToDate = true
	}
	if args.LastLogTerm == rf.lastTerm() && args.LastLogIndex >= rf.lastIndex() {
		upToDate = true
	}

	reply.Term = rf.currentTerm
	if upToDate && (rf.voteFor == None || rf.voteFor == args.CandidateID) {
		reply.IsVoteGranted = true
		rf.role = Follower // 这句让 2A-2 Passed？
		rf.voteFor = args.CandidateID
		rf.currentTerm = args.Term
	}
	//fmt.Println("rv", rf.me, rf.voteFor)

}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	//fmt.Println("Peer", rf.me, "started rpc RequestVote", args.Term)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	//fmt.Println("Peer", rf.me, "ended rpc RequestVote", reply.Term)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if ok {
		if rf.role != Candidate || rf.currentTerm != args.Term {
			return ok
		}

		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.role = Follower
			rf.voteFor = None
		}

		if reply.IsVoteGranted {
			rf.voteCount++
			if rf.voteCount > len(rf.peers)/2 {
				rf.leaderCh <- true
			}
		}
	}
	return ok
}
