package raft

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogTerm  int
	PrevLogIndex int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term      int
	Success   bool
	NextIndex int // 第一个日志项不冲突的 index
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 任期小，忽略
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	// 任期大，无条件转为 Follower
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.role = Follower
		rf.voteFor = None
	}

	rf.heartbeatCh <- true
	rf.currentTerm = args.Term
	reply.Term = rf.currentTerm

	if args.PrevLogIndex > rf.lastIndex() || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Success = false // 日志冲突，返回 false

	}
	reply.Success = true

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
