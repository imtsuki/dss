package raft

import "fmt"

type AppendEntriesArgs struct {
	Term         int        // Leader 的 term
	LeaderID     int        // Leader 的 ID
	PrevLogTerm  int        // 新的日志项之前一项的 term
	PrevLogIndex int        // 新的日志项之前一项的 index
	Entries      []LogEntry // 准备提交的日志项（空为心跳）
	LeaderCommit int        // Leader 的 commitIndex
}

type AppendEntriesReply struct {
	Term      int  // currentTerm，用于 Leader 更新
	Success   bool // true if PrevLogIndex & PrevLogTerm matched
	NextIndex int  // 第一个日志项不冲突的 index
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer func() {
		if !reply.Success {
			fmt.Println("Rejected", rf.me)
		}
	}()

	// 任期小，忽略
	if args.Term < rf.currentTerm {
		reply.NextIndex = rf.lastIndex() + 1
		reply.Term = rf.currentTerm
		reply.Success = false
		fmt.Println("Rejected1")
		return
	}

	// 任期大，无条件转为 Follower（所有服务器收到所有 RPC）
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.role = Follower
		rf.voteFor = None
	}

	rf.heartbeatCh <- true
	//rf.currentTerm =
	reply.Term = args.Term

	if args.PrevLogIndex > rf.lastIndex() {
		reply.Success = false // 日志冲突，返回 false
		reply.NextIndex = rf.lastIndex() + 1
		fmt.Println("Rejected2")
		return
	}

	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Success = false // 日志冲突，返回 false
		//nextIndex := min(rf.lastIndex(), args.PrevLogIndex)
		//conflictTerm := rf.log[nextIndex].Term
		//for ; nextIndex > rf.commitIndex && rf.log[nextIndex].Term == conflictTerm; nextIndex-- {
		//}
		//reply.NextIndex = nextIndex
		//return
		for i := args.PrevLogIndex - 1; i >= 0; i-- {
			if rf.log[i].Term != rf.log[args.PrevLogIndex].Term {
				reply.NextIndex = i + 1
				break
			}
		}
		fmt.Println("Rejected3")
		return

	}

	reply.Success = true
	rf.log = rf.log[:args.PrevLogIndex+1]
	rf.log = append(rf.log, args.Entries...)
	reply.NextIndex = rf.lastIndex() + 1

	//rf.commitIndex = min(args.LeaderCommit, rf.lastIndex())
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.lastIndex())
		rf.commitCh <- true
	}

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
			return ok
		}
		if reply.Success {
			if len(args.Entries) > 0 {
				rf.nextIndex[server] = args.Entries[len(args.Entries)-1].Index + 1
				rf.matchIndex[server] = rf.nextIndex[server] - 1
				//fmt.Println("result", server, rf.nextIndex[server])
			}

		} else {
			// 冲突，nextIndex 回退
			rf.nextIndex[server] = reply.NextIndex
		}
	}

	return ok
}
