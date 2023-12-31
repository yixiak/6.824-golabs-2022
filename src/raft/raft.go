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
	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// The role played by nodes
type State int

const (
	Follower  State = 0
	Candidate State = 1
	Leader    State = 2
)

type logEntry struct {
	Term    int
	Index   int
	Command interface{}
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	state State

	currentTerm       int
	votedFor          int
	logs              []*logEntry
	commitIndex       int
	lastApplied       int // Index of the last applied logEntry, start from 1
	nextIndex         []int
	matchIndex        []int
	applyMsg          chan ApplyMsg
	election_timeout  *time.Timer
	heartbeat_timeout *time.Timer
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm
	isleader = rf.state == Leader
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
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

// restore previously persisted state.
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

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	TERM         int
	CANDIDATEID  int
	LASTLOGINDEX int
	LASTLOGTERM  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	TERM        int
	VOTEGRANTED bool
}

type AppendEntriesArgs struct {
	TERM         int
	LEADERID     int
	PREVLOGINDEX int
	PREVLOGTERM  int
	ENTRIES      []*logEntry
	LEADERCOMMIT int
}

type AppendEntriesReply struct {
	TERM    int
	SUCCESS bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	DPrintf("[Server %v] receive a RequestVote from %v with %v.term is %v, %v's term is %v\n\t\t\tand rf.lastApply is %v, args.lastlogIndex is %v", rf.me, args.CANDIDATEID, args.CANDIDATEID, args.TERM, rf.me, rf.currentTerm, rf.lastApplied, args.LASTLOGINDEX)
	reply.TERM = rf.currentTerm
	// Default not to vote
	reply.VOTEGRANTED = false
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.currentTerm > args.TERM || (rf.currentTerm == args.TERM && rf.votedFor != -1 && rf.votedFor != args.CANDIDATEID && rf.votedFor != rf.me) || !rf.isLogUptoDate(args.LASTLOGINDEX, args.LASTLOGTERM) {
		// has voted to another Candidate
		DPrintf("[Server %v] DO NOT vote to %v, rf.votedFor is %v", rf.me, args.CANDIDATEID, rf.votedFor)
		return
	}
	if rf.currentTerm <= args.TERM {
		if rf.currentTerm == args.TERM {
			if !rf.isLogUptoDate(args.LASTLOGINDEX, args.LASTLOGTERM) {
				return
			}
		}
		DPrintf("[Server %v] vote to %v", rf.me, args.CANDIDATEID)
		reply.VOTEGRANTED = true
		rf.votedFor = args.CANDIDATEID
		rf.currentTerm = args.TERM
		if rf.state == Candidate || rf.state == Leader {
			// go back to Follower
			reply.VOTEGRANTED = true
			rf.state = Follower
		}
	}

}

// if the request log is more up-to-date than this client
func (rf *Raft) isLogUptoDate(lastLogIndex int, lastLogTerm int) bool {
	if rf.lastApplied == 0 {
		return true
	} else {
		DPrintf("[Server %v] checking isn't logUptoDate: lastLogIndex is %v, lastLogTerm is %v", rf.me, lastLogIndex, lastLogTerm)
		//if rf.logs[rf.lastApplied].Term > lastLogTerm || rf.lastApplied > lastLogIndex {
		//	return false
		//}
		if rf.logs[len(rf.logs)-1].Term > lastLogTerm {
			return false
		}
		if rf.logs[len(rf.logs)-1].Term == lastLogTerm {
			if len(rf.logs)-1 > lastLogIndex {
				return false
			}
		}
	}
	return true
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {

	reply.TERM = rf.currentTerm
	reply.SUCCESS = false
	if args.TERM < reply.TERM {
		return
	}
	if args.PREVLOGINDEX > len(rf.logs) {
		DPrintf("[Server %v] return false to %v's AppendEntries for arg.prevlogindex %v > len(log)", rf.me, args.LEADERID, args.PREVLOGINDEX)
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// Receiver implementation
	prevLogIndex := args.PREVLOGINDEX
	if prevLogIndex >= len(rf.logs) {
		return
	}
	if args.PREVLOGINDEX >= 0 && rf.logs[args.PREVLOGINDEX].Term != args.PREVLOGTERM {
		DPrintf("[Server %v] Leader's term is different", rf.me)
		return
	}
	if len(args.ENTRIES) == 0 {
		// receive a heartbeat
		DPrintf("[Server %v] receive a empty Entry from %v", rf.me, args.LEADERID)
		rf.election_timeout.Reset(RandElectionTimeout())
		reply.SUCCESS = true
		// Leader update its commitIndex after commit itself
		if args.LEADERCOMMIT > rf.commitIndex {
			DPrintf("[Server %v] receive a LEADERCOMMIT %v ", rf.me, args.LEADERCOMMIT)
			if args.LEADERCOMMIT > len(rf.logs)-1 {
				rf.commitIndex = len(rf.logs) - 1
			} else {
				rf.commitIndex = args.LEADERCOMMIT
			}
			rf.apply()
			DPrintf("[Server %v] update its commitIndex to %v ", rf.me, rf.commitIndex)
		}
		return
	}
	rf.election_timeout.Reset(RandElectionTimeout())
	DPrintf("[Server %v] receive appendentries with entry %+v. And currentTerm is %v, logs len is %v, committedIndex is: %v", rf.me, args.ENTRIES[0], rf.currentTerm, len(rf.logs), rf.commitIndex)

	rf.logs = rf.logs[:prevLogIndex+1]
	rf.logs = append(rf.logs, args.ENTRIES...)
	DPrintf("[Server %v]'s loglen is %v after append", rf.me, len(rf.logs))
	if args.LEADERCOMMIT > rf.commitIndex {
		DPrintf("[Server %v]'s commit Index is less then Leader's", rf.me)
		if args.LEADERCOMMIT > len(rf.logs)-1 {
			rf.commitIndex = len(rf.logs) - 1
		} else {
			rf.commitIndex = args.LEADERCOMMIT
		}
		rf.apply()
	}

	DPrintf("[Server %v]'s commited Index is %v", rf.me, rf.commitIndex)
	DPrintf("[Server %v]'s lastapplied log is %+v now", rf.me, rf.logs[rf.commitIndex])
	reply.SUCCESS = true
}

// apply the entry when commitIndex > LastApply
func (rf *Raft) apply() {

	for rf.commitIndex > rf.lastApplied && rf.lastApplied+1 < len(rf.logs) {
		rf.lastApplied++
		DPrintf("[Server %v] is applying %v: %+v", rf.me, rf.lastApplied, rf.logs[rf.lastApplied])
		applymsg := ApplyMsg{
			Command:      rf.logs[rf.lastApplied].Command,
			CommandIndex: rf.lastApplied,
			CommandValid: true,
		}
		rf.applyMsg <- applymsg
	}

}

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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	isLeader = rf.state == Leader
	if isLeader {
		DPrintf("[Server %v] receive a command {%v}, commitIndex is %v", rf.me, command, rf.commitIndex)
		rf.mu.Lock()
		term = rf.currentTerm
		index = len(rf.logs) // index begin from 1
		rf.logs = append(rf.logs, &logEntry{
			term,
			index,
			command,
		})
		rf.SendNewCommandToAll()
		rf.mu.Unlock()
	}

	return index, term, isLeader
}

func (rf *Raft) SendNewCommandToAll() {
	DPrintf("[Server %v] is sending new command to others", rf.me)
	commitNum := 1
	commitNumLock := sync.Mutex{}
	oldCommit := rf.commitIndex
	for server := range rf.peers {
		if server == rf.me {
			continue
		}
		go func(peer int) {
			var prevterm = 0
			if rf.nextIndex[peer]-1 > 0 {
				prevterm = rf.logs[rf.nextIndex[peer]-1].Term
			}
			entry := make([]*logEntry, len(rf.logs[rf.nextIndex[peer]:]))
			args := &AppendEntriesArgs{
				TERM:         rf.currentTerm,
				LEADERID:     rf.me,
				LEADERCOMMIT: rf.commitIndex,
				PREVLOGINDEX: rf.nextIndex[peer] - 1,
				PREVLOGTERM:  prevterm,
				ENTRIES:      entry,
			}
			copy(args.ENTRIES, rf.logs[rf.nextIndex[peer]:])

			DPrintf("[Server %v] the ENTRY with prevlogIndex %v for Server %v", rf.me, args.PREVLOGINDEX, peer)

			reply := &AppendEntriesReply{}
			if rf.sendAppendEntries(peer, args, reply) {

				if reply.TERM > rf.currentTerm {
					rf.mu.Lock()
					rf.currentTerm = reply.TERM
					rf.state = Follower
					rf.election_timeout.Reset(RandElectionTimeout())
					rf.votedFor = -1
					rf.mu.Unlock()
					return
				}
				if reply.SUCCESS {
					rf.mu.Lock()
					rf.matchIndex[peer] = args.PREVLOGINDEX + len(args.ENTRIES)
					rf.nextIndex[peer] = rf.matchIndex[peer] + 1
					DPrintf("[Server %v] update %v nextIndex and matchIndex to %v %v,and rf.commitIndex is %v，old is %v", rf.me, peer, rf.nextIndex[peer], rf.matchIndex[peer], rf.commitIndex, oldCommit)
					commitNumLock.Lock()
					commitNum++
					if rf.commitIndex == oldCommit && commitNum >= (len(rf.peers)+1)/2 {
						rf.commitIndex++
						DPrintf("[Server %v] commit a new command with commitId %v: %+v", rf.me, rf.commitIndex, rf.logs[rf.commitIndex])
						rf.apply()
						rf.SendheartbeatToAll()
						rf.heartbeat_timeout.Reset(20 * time.Millisecond)
					}
					commitNumLock.Unlock()
					rf.mu.Unlock()
				} else {
					rf.mu.Lock()
					rf.nextIndex[peer]--
					DPrintf("[Server %v] update %v nextIndex to %v", rf.me, peer, rf.nextIndex[peer])
					rf.mu.Unlock()
				}
			} else {
				DPrintf("[Server %v] lost the connection with %v", rf.me, peer)
			}
		}(server)
	}
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func RandElectionTimeout() time.Duration {
	source := rand.NewSource(time.Now().UnixMicro())
	ran := rand.New(source)
	//DPrintf("A new randElectionTimeout: %v", time.Duration(150+ran.Int()%150)*time.Millisecond)
	return time.Duration(500+ran.Int()%150) * time.Millisecond
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		rf.mu.Lock()
		currentstate := rf.state
		rf.mu.Unlock()
		select {
		case <-rf.heartbeat_timeout.C:
			rf.mu.Lock()
			if currentstate == Leader {
				// send heartbeat to all followers
				rf.SendheartbeatToAll()
				rf.heartbeat_timeout.Reset(20 * time.Millisecond)
			}
			rf.mu.Unlock()
		case <-rf.election_timeout.C:
			rf.mu.Lock()
			rf.currentTerm += 1
			rf.state = Candidate
			// start an election event
			rf.StartElection()
			rf.election_timeout.Reset(RandElectionTimeout())
			rf.mu.Unlock()
		}
	}

}

func (rf *Raft) SendheartbeatToAll() {
	DPrintf("[Server %v] send heartbeat", rf.me)
	if rf.state == Leader {
		for server := range rf.peers {
			if server != rf.me {
				prevIndex := rf.nextIndex[server] - 1
				DPrintf("[Server %v] send heartbeat to %v with prevIndex %v", rf.me, server, prevIndex)
				if prevIndex < 0 {
					DPrintf("[Server %v] don't send heartbeat to %v", rf.me, server)
					continue
				}
				prevTerm := rf.logs[prevIndex].Term
				entries := make([]*logEntry, len(rf.logs)-prevIndex-1)
				copy(entries, rf.logs[prevIndex+1:])
				args := &AppendEntriesArgs{
					TERM:         rf.currentTerm,
					LEADERID:     rf.me,
					ENTRIES:      entries,
					LEADERCOMMIT: rf.commitIndex,
					PREVLOGINDEX: prevIndex,
					PREVLOGTERM:  prevTerm,
				}

				reply := new(AppendEntriesReply)
				go func(peer int) {
					if rf.sendAppendEntries(peer, args, reply) {
						if reply.SUCCESS {
							rf.matchIndex[peer] = args.PREVLOGINDEX + len(args.ENTRIES)
							rf.nextIndex[peer] = rf.matchIndex[peer] + 1
							DPrintf("[Server %v] update %v nextIndex and matchIndex to %v %v", rf.me, peer, rf.nextIndex[peer], rf.matchIndex[peer])
							// update the commit index

							toCommit := make([]int, len(rf.logs))
							for index := range rf.peers {
								com := rf.matchIndex[index]
								toCommit[com]++
							}
							peerLen := len(rf.peers)
							// find the largest index which can be committed (at least larger than old commitIndex)
							sum := 0
							for i := len(toCommit) - 1; i > rf.commitIndex; i-- {
								sum += toCommit[i]
								if sum >= (1+peerLen)/2 {
									rf.commitIndex = i
									rf.apply()
									DPrintf("[Server %v] commitIndex is %v", rf.me, rf.commitIndex)
									break
								}
							}

						} else {
							// there is a client with larger term
							if reply.TERM > rf.currentTerm {
								rf.state = Follower
								rf.election_timeout.Reset(RandElectionTimeout())
							} else {
								// there is no matching entry
								rf.nextIndex[peer] -= 1
								DPrintf("[Server %v] update %v nextIndex to %v", rf.me, peer, rf.nextIndex[peer])
							}
						}
					} else {
						// Loss of connection
						DPrintf("[Server %v] lost connection with %v", rf.me, peer)
					}
				}(server)
			}
		}
	}
}

func (rf *Raft) StartElection() {
	DPrintf("[Server %v] start an election event", rf.me)
	rf.votedFor = rf.me
	logLen := len(rf.logs)
	agreeNum := 1 // itself
	// maybe there is no entry in log
	lastLogIndex := logLen - 1
	lastLogTerm := 0
	if logLen > 1 {
		//fmt.Printf("loglen is %v\n", logLen)
		lastLogTerm = rf.logs[logLen-1].Term
	}
	args := &RequestVoteArgs{
		TERM:         rf.currentTerm,
		CANDIDATEID:  rf.me,
		LASTLOGTERM:  lastLogTerm,
		LASTLOGINDEX: lastLogIndex,
	}
	agreeNumLock := sync.Mutex{}
	for server := range rf.peers {
		go func(peer int) {
			if peer != rf.me {
				reply := new(RequestVoteReply)
				// receive the reply
				if rf.sendRequestVote(peer, args, reply) {
					rf.mu.Lock()
					defer rf.mu.Unlock()
					if rf.currentTerm == args.TERM && rf.state == Candidate {

						//DPrintf("[Server %v] Receive reply from %v", rf.me, peer)
						if reply.VOTEGRANTED {
							// get a vote
							DPrintf("[Server %v] get a vote from %v", rf.me, peer)
							agreeNumLock.Lock()
							agreeNum++

							// win this election and then send heartbeat, interrupt sending election message
							if agreeNum >= (len(rf.peers)+1)/2 {
								DPrintf("[Server %v] become a new leader", rf.me)
								rf.TobeLeader()
							}
							agreeNumLock.Unlock()
							return
						} else if reply.TERM > rf.currentTerm {
							// find another Candidate/leader
							//DPrintf("[Server %v] find a larger term from %v", rf.me, peer)
							//rf.mu.Lock()
							//defer rf.mu.Unlock()
							rf.currentTerm = reply.TERM
							// go back to Follower and interrupt sending message
							rf.state = Follower
							rf.election_timeout.Reset(RandElectionTimeout())
							rf.votedFor = -1
						}
					}
				}
			}
		}(server)
	}
}

func (rf *Raft) TobeLeader() {

	rf.election_timeout.Reset(RandElectionTimeout())
	if len(rf.logs) > 0 {
		DPrintf("[Server %v] to be a leader with logs len: %v , term: %v and commitIndex: %v", rf.me, len(rf.logs), rf.currentTerm, rf.commitIndex)
	}
	rf.state = Leader
	lastIndex := len(rf.logs)
	for peer := range rf.nextIndex {
		rf.nextIndex[peer] = lastIndex
		rf.matchIndex[peer] = 0
	}

	rf.SendheartbeatToAll()
	rf.heartbeat_timeout.Reset(20 * time.Millisecond)
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	iniEntry := &logEntry{
		Term:    0,
		Index:   0,
		Command: "FAIL",
	}
	logs := make([]*logEntry, 0)
	logs = append(logs, iniEntry)
	rf := &Raft{
		peers:     peers,
		persister: persister,
		me:        me,
		state:     Follower,
		// generate a rand election time between 150ms~300ms
		election_timeout:  time.NewTimer(RandElectionTimeout()),
		heartbeat_timeout: time.NewTimer(20 * time.Millisecond),
		currentTerm:       0,
		votedFor:          -1,
		commitIndex:       0,
		lastApplied:       0,
		nextIndex:         make([]int, len(peers)),
		matchIndex:        make([]int, len(peers)),
		logs:              logs,
		applyMsg:          applyCh,
	}
	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
