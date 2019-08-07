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

	if args.Term < rf.currentTerm {
		reply.IsVoteGranted = false
		reply.Term = rf.currentTerm

		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.role = Follower
		rf.voteFor = None
		reply.Term = rf.currentTerm
		return
	}
	reply.Term = rf.currentTerm
	if rf.voteFor == None || rf.voteFor == args.CandidateID {
		reply.IsVoteGranted = true
		rf.role = Follower // 这句让 2A-2 Passed？
		rf.voteFor = args.CandidateID
		rf.currentTerm = args.Term
	}
	//fmt.Println("rv", rf.me, rf.voteFor)

}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
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

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogTerm  int
	PrevLogIndex int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	rf.heartbeatCh <- true
	rf.currentTerm = args.Term
	reply.Success = true
	reply.Term = rf.currentTerm

}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if ok {
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.role = Follower
			rf.voteFor = None
		}
	}

	return ok
}
