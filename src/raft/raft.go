package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"labrpc"
	"sync"
	"time"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

// Role is the state of the Raft peer (Follower, Leader, Candidate).
type Role int

const (
	Follower Role = iota
	Leader
	Candidate
)

type LogEntry struct {
	Term    int
	Index   int
	Command interface{}
}

// None is used for Option type
const None = -1

// HeartbeatInterval for every 50ms (Leader)
const HeartbeatInterval = 50 * time.Millisecond

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	role Role // 节点状态

	applyCh     chan ApplyMsg
	heartbeatCh chan bool
	leaderCh    chan bool
	commitCh    chan bool

	voteCount int

	// Persistent state on all servers:
	currentTerm int        // 服务器最后一次知道的任期号（初始化为 0，持续递增）
	voteFor     int        // 在当前获得选票的候选人的 Id (Option<int>)
	log         []LogEntry // 日志条目集；每一个条目包含一个用户状态机执行的指令，和收到时的任期号

	// Volatile state on all servers:
	commitIndex int // 已知的最大的已经被提交的日志条目的索引值
	lastApplied int // 最后被应用到状态机的日志条目索引值（初始化为 0，持续递增）

	// Volatile state on leaders: 选举后重新初始化
	nextIndex  []int // 对于每一个服务器，需要发送给他的下一个日志条目的索引值（初始化为 Leader LastIndex 加一）
	matchIndex []int // 对于每一个服务器，已经复制给他的日志的最高索引值（初始化为 0）
}

func (rf *Raft) lastIndex() int {
	return rf.log[len(rf.log)-1].Index
}
func (rf *Raft) lastTerm() int {
	return rf.log[len(rf.log)-1].Term
}

// GetState returns currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term = rf.currentTerm
	isleader = rf.role == Leader

	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index := None
	term := rf.currentTerm
	isLeader := rf.role == Leader

	// Your code here (2B).
	if isLeader {
		index = rf.lastIndex() + 1
		//fmt.Println("Start on, index", rf.me, index)
		rf.log = append(rf.log, LogEntry{Term: term, Index: index, Command: command})
	}

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.role = Follower //所有节点开始都是 Follower
	rf.applyCh = applyCh
	rf.heartbeatCh = make(chan bool, 64)
	rf.leaderCh = make(chan bool, 64)
	rf.commitCh = make(chan bool, 64)

	// Persistent state on all servers:
	rf.currentTerm = 0
	rf.voteFor = None
	rf.log = append(rf.log, LogEntry{Term: 0})

	// Volatile state on all servers:
	rf.commitIndex = 0
	rf.lastApplied = 0

	// Volatile state on leaders:
	rf.nextIndex = make([]int, 0)
	rf.matchIndex = make([]int, 0)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	//fmt.Println("init")
	go rf.Serve()

	go func() {
		for {
			select {
			case <-rf.commitCh:
				rf.mu.Lock()
				commitIndex := rf.commitIndex
				for i := rf.lastApplied + 1; i <= commitIndex; i++ {
					msg := ApplyMsg{CommandIndex: i, Command: rf.log[i].Command, CommandValid: true}
					applyCh <- msg
					//fmt.Printf("me:%d %v\n",rf.me,msg)
					rf.lastApplied = i
				}
				//fmt.Print(rf.me, "now is")
				//for i := range rf.log {
				//	fmt.Print(rf.log[i])
				//}
				//fmt.Println()
				rf.mu.Unlock()
			}
		}
	}()
	return rf
}

// Serve runs Raft's state machine.
func (rf *Raft) Serve() {
	for {
		switch rf.role {
		case Follower:
			select {
			case <-rf.heartbeatCh:
			case <-time.After(between(300, 500)):
				rf.role = Candidate
			}
		case Leader:
			rf.broadcastAppendEntries() // 发送心跳
			time.Sleep(HeartbeatInterval)
		case Candidate:
			rf.mu.Lock()
			rf.currentTerm++
			rf.voteFor = rf.me
			rf.voteCount = 1
			rf.mu.Unlock()
			go rf.startElection() // 开始选举
			// 接收选举结果
			select {
			// 收到了其他节点发来的心跳，自己变为 Follower
			case <-rf.heartbeatCh:
				rf.mu.Lock()
				rf.role = Follower
				rf.mu.Unlock()
			// 选举成功
			case <-rf.leaderCh:
				rf.mu.Lock()
				rf.role = Leader
				//fmt.Println("I am leader", rf.me, "term is", rf.currentTerm)
				rf.nextIndex = make([]int, len(rf.peers))
				rf.matchIndex = make([]int, len(rf.peers))

				for i := range rf.peers {
					// 刚成为 Leader 时，初始化为 Leader 的 LastIndex + 1
					rf.nextIndex[i] = rf.lastIndex() + 1
					// 初始化为 0
					rf.matchIndex[i] = 0
				}
				rf.mu.Unlock()
			// 没有结果，超时
			case <-time.After(between(200, 400)):
			}
		}
	}
}

func (rf *Raft) startElection() {

	var args RequestVoteArgs
	rf.mu.Lock()
	//fmt.Println(rf.me, "election", rf.currentTerm)

	args.Term = rf.currentTerm
	args.CandidateID = rf.me
	args.LastLogIndex = rf.lastIndex()
	args.LastLogTerm = rf.lastTerm()
	rf.mu.Unlock()

	for i := range rf.peers {
		if i != rf.me && rf.role == Candidate {
			go func(i int, args RequestVoteArgs) {
				//fmt.Println("I am", rf.me, "request", i, args)
				var reply RequestVoteReply
				rf.sendRequestVote(i, &args, &reply)
			}(i, args)
		}
	}
}

func (rf *Raft) broadcastAppendEntries() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//fmt.Println(rf.me, "heartbeat", rf.currentTerm)

	//
	N := rf.commitIndex

	for i := rf.commitIndex + 1; i <= rf.lastIndex(); i++ {
		num := 1
		for j := range rf.peers {
			if j != rf.me && rf.matchIndex[j] >= i && rf.log[i].Term == rf.currentTerm {
				num++
			}
		}
		if num > len(rf.peers)/2 {
			N = i
		}
	}

	if N > rf.commitIndex {
		rf.commitIndex = N
		rf.commitCh <- true
	}

	for i := range rf.peers {
		if i != rf.me && rf.role == Leader {
			args := AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderID:     rf.me,
				PrevLogIndex: rf.nextIndex[i] - 1,
				PrevLogTerm:  rf.log[rf.nextIndex[i]-1].Term,
				LeaderCommit: rf.commitIndex,
			}
			args.Entries = make([]LogEntry, len(rf.log[args.PrevLogIndex+1:]))
			copy(args.Entries, rf.log[args.PrevLogIndex+1:])

			go func(server int, args AppendEntriesArgs) {
				var reply AppendEntriesReply
				if len(args.Entries) > 0 {
					//fmt.Println(args, "send to", server)
				}
				rf.sendAppendEntries(server, &args, &reply)
			}(i, args)
		}
	}
}
