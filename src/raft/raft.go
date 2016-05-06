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

import "sync"
import "sync/atomic"

import "labrpc"

import "time"
import "math/rand"

import "bytes"
import "encoding/gob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

//
// A Go object implementing a single Raft peer.
//

//
// LogEntry
//
type LogEntry struct {
	Term    int
	Command interface{}
}

type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]
	log       []LogEntry

	applyChan chan ApplyMsg // command queue

	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	// @every server
	isDead bool

	currentTerm int
	votedFor    int
	commitIndex int // index of highest log entry known to be commited
	lastApplied int // index of entry applied to local state machine
	// @leader
	nextIndex  []int // index of highest log entry known to be commited
	matchIndex []int // index of
	// stopLastSendEntry chan int

	// role transfer sig chan, personal design
	// voteCountChan    chan int // term as element, for vote counting
	// toLeaderChan     chan int // from candidate to leader
	// toFollowerChan   chan int // from leader to follower
	stayFollowerChan chan int // stay as follower

	// index to count
	index2cnt map[int] int // for update of commit index

	// client interaction
	// requestChan chan interface{}

	// kill signal
	killSigChan chan int // rf.Kill signal
	closeChanChan chan int // close channel

	role int32 // 0: follower, 1: candidate, 2: leader
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	isleader := false
	rf.mu.Lock()
	term = rf.currentTerm
	if (rf.role == 2) {
		isleader = true
	}
	rf.mu.Unlock()
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here.
	// Example:
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)

	e.Encode(rf.log)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.commitIndex)
	e.Encode(rf.lastApplied)

	// e.Encode(rf.nextIndex)
	// e.Encode(rf.matchIndex)

	data := w.Bytes()
	if data==nil {
		DPrintf("rf[%v].persist: nil data\n", rf.me)
	}
	rf.persister.SaveRaftState(data)
	// DPrintf("rf[%v].persist: data:%v\n", rf.me, rf.persister.raftstate)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here.
	// Example:
	if data == nil {
		DPrintf("rf[%v].readPersist: nil data\n", rf.me)
		return
	}

	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)

	d.Decode(&rf.log)
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.commitIndex)
	d.Decode(&rf.lastApplied)

	// d.Decode(&rf.nextIndex)
	// d.Decode(&rf.matchIndex)
}

//
// example RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
	// Your data here.
	Term         int // candidate term
	CandidateId  int // who am i
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	// Your data here.
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate won the vote
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {

	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	if rf.isDead {
		DPrintf("dead server: %v, ignore, reply:%v\n", rf.me, reply.VoteGranted)
		return
	}

	if args.Term < rf.currentTerm {
		return
	}
	if rf.role == 0 {
		rf.stayFollowerChan <- rf.currentTerm
	}

	if args.Term > rf.currentTerm {
		DPrintf("vote: %v to %v: term: args.Term=%v > rf.currentTerm=%v\n", args.CandidateId, rf.me, args.Term, rf.currentTerm)
		// DPrintf("AppendEntries: %v to %v: term: args.Term=%v > rf.currentTerm=%v\n", args.LeaderId, rf.me, args.Term, rf.currentTerm)
		rf.currentTerm = args.Term
		rf.votedFor = -1 // voteFor tied with currentTerm
		rf.persist()
		if rf.role != 0 { // only become follower once
			rf.role = 0
			go rf.AsFollower()
		}
	}

	if rf.votedFor == -1 { // only vote once
		// 2, cmp log `up to date`
		lastIdx := len(rf.log) - 1
		lastTerm := -1
		if lastIdx > -1 {
			lastTerm = rf.log[lastIdx].Term
		}
		DPrintf("RequestVote rf[%v].LastLogTerm: %v, args.LastLogTerm:%v, rf[%v].lastLogIndex: %v args.LastLogIndex:%v\n", rf.me, lastTerm, args.LastLogTerm, rf.me, lastIdx, args.LastLogIndex)
		if len(rf.log) == 0 || args.LastLogTerm > lastTerm || (lastTerm == args.LastLogTerm && args.LastLogIndex >= lastIdx) {
			reply.VoteGranted = true
			rf.votedFor = args.CandidateId
		}
		DPrintf("RequestVote %v to %v, rf[%v].Term: %v, rf[%v].log: %v, voteGrant:%v\n", args.CandidateId, rf.me, rf.me, rf.currentTerm, rf.me, rf.log, reply.VoteGranted)
	}
	return
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should probably
// pass &reply.
//
// returns true if labrpc says the RPC was delivered.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

type AppendEntriesArgs struct {
	Term         int         // currentTerm for leader
	LeaderId     int         //
	PrevLogTerm  int         // for consistence check
	PrevLogIndex int         // for consistence check
	PrevLogEntry LogEntry    // PrevLogEntry
	Entries      []LogEntry // entries to be appended to each follower's log
	LeaderCommit int         // ?
}

type AppendEntriesReply struct {
	Term    int
	Success bool
	NextIndex int
}

func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {

	reply.Term = rf.currentTerm
	reply.Success = false
	reply.NextIndex = 0

	var idx1 int
	var idx2 int

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.isDead {
		DPrintf("dead server: %v, ignore, reply:%v\n", rf.me, reply.Success)
		return
		DPrintf("you can't see me\n")
	}

	if args.Term < rf.currentTerm {
		return
	}
	if atomic.LoadInt32(&(rf.role)) == 0 {
		rf.stayFollowerChan <- rf.currentTerm
	}
	if args.Term > rf.currentTerm {
		DPrintf("AppendEntries: %v to %v: term: args.Term=%v > rf.currentTerm=%v\n", args.LeaderId, rf.me, args.Term, rf.currentTerm)
		rf.currentTerm = args.Term
		rf.votedFor = -1 // voteFor tied with currentTerm
		rf.persist()

		if rf.role != 0 { // only become follower once
			rf.role = 0
			go rf.AsFollower()
			// rf.toFollowerChan <- rf.currentTerm
		}
	}

	if args.PrevLogIndex < -1 {
		reply.NextIndex = 0
		DPrintf("%v to %v: AppendEntries < -1, args.PrevLogIndex: %v, args.PrevLogTerm: %v update NextIndex: %v rf[%v].log: %v\n", args.LeaderId, rf.me, args.PrevLogIndex, args.PrevLogTerm, reply.NextIndex, rf.me, rf.log)
		return
	} else if args.PrevLogIndex >= len(rf.log) {
		reply.NextIndex = len(rf.log)
		for reply.NextIndex > 1 && rf.log[reply.NextIndex-2].Term == rf.log[reply.NextIndex-1].Term {
			reply.NextIndex--
		}
		DPrintf("%v to %v: AppendEntries longer, args.PrevLogIndex: %v, args.PrevLogTerm: %v update NextIndex: %v rf[%v].log: %v\n", args.LeaderId, rf.me, args.PrevLogIndex, args.PrevLogTerm, reply.NextIndex, rf.me, rf.log)
		return
	} else if (args.PrevLogIndex >=0 && (rf.log[args.PrevLogIndex].Term != args.PrevLogEntry.Term || rf.log[args.PrevLogIndex].Command != args.PrevLogEntry.Command)) {
		reply.NextIndex = args.PrevLogIndex
		for reply.NextIndex > 0 && rf.log[reply.NextIndex-1].Term == rf.log[args.PrevLogIndex].Term {
			reply.NextIndex--
		}
		DPrintf("%v to %v: AppendEntries mismatch, args.PrevLogIndex: %v, args.PrevLogTerm: %v update NextIndex: %v rf[%v].log: %v\n", args.LeaderId, rf.me, args.PrevLogIndex, args.PrevLogTerm, reply.NextIndex, rf.me, rf.log)
		return
	}

	// 3, append entries if necessary
	// 3.1, delete forward entries
	// var idx2 int
	unchanged := true
	for idx1, idx2 = args.PrevLogIndex + 1, 0; idx1 < len(rf.log) && idx2 < len(args.Entries); idx1++ {
		if rf.log[idx1].Term == args.Entries[idx2].Term && rf.log[idx1].Command == args.Entries[idx2].Command {
				idx2++
				continue
      } else {
				rf.log[idx1] = args.Entries[idx2]
				unchanged = false
      }
      idx2++
	}
	DPrintf("%v to %v: AppendEntries, args.PrevLogIndex: %v, args.PrevLogTerm: %v update NextIndex: %v, idx1:%v, idx2:%v, len(rf[%v].log): %v, len(args.Entries): %v\n", args.LeaderId, rf.me, args.PrevLogIndex, args.PrevLogTerm, reply.NextIndex, idx1, idx2, rf.me, len(rf.log), len(args.Entries))
	if len(rf.log) > idx1 { // idx2 up; idx1 not
		rf.log = rf.log[:idx1]
		unchanged = false
	}
	if len(rf.log) == idx1 && len(args.Entries) > idx2 { // idx1 up; idx2 not
		rf.log = append(rf.log, args.Entries[idx2:]...)
		unchanged = false
	}
	DPrintf("%v to %v: AppendEntries, args.PrevLogIndex: %v, args.PrevLogTerm: %v update NextIndex: %v, idx1:%v, idx2:%v, rf[%v].log: %v, args.Entries: %v\n", args.LeaderId, rf.me, args.PrevLogIndex, args.PrevLogTerm, reply.NextIndex, idx1, idx2, rf.me, rf.log, args.Entries)
	if len(rf.log) > 0 && unchanged && len(args.Entries) > 0 { // !!! len(rf.log)==0
		reply.NextIndex = len(rf.log)
		reply.Success = false
		return
	}

	rf.persist()

	// 3.3, update commit index as min(args.LeaderCommit, lastIndex)
	// verify cauz `Log Matching Property`
	if args.LeaderCommit > rf.commitIndex {
		lastCommitIndex := rf.commitIndex
		lastIndex := len(rf.log) - 1
		if rf.commitIndex = lastIndex; lastIndex > args.LeaderCommit {
			rf.commitIndex = args.LeaderCommit
		}
		rf.persist()
		DPrintf("%v to %v: rf.commitIndex updated from %v - %v, rf[%v].log:%v\n", args.LeaderId, rf.me, lastCommitIndex, rf.commitIndex, rf.me, rf.log)
		// go func () {
		for i := lastCommitIndex + 1; i < rf.commitIndex + 1; i++ {
				msg := ApplyMsg{Command: rf.log[i].Command, Index: i, UseSnapshot: false, Snapshot: nil}
				rf.applyChan <- msg
				DPrintf("AppendEntries: server:%v applyChan:%v\n", rf.me, msg)
		}
		// } ()
	}
	reply.Success = true
}

func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (index int, term int, isLeader bool) {
	rf.mu.Lock()

	index = len(rf.log) // the index that the command will appear at if it's ever committed.
	term = rf.currentTerm // the current term

	isLeader = true
	if rf.role != 2 { // if not leader, return immediately
		isLeader = false
		rf.mu.Unlock()
		return
	}

	/* in case of `concurrent start`, append entry here*/
	currEntry := LogEntry{Command: command, Term: rf.currentTerm}
	rf.log = append(rf.log, currEntry)
	rf.index2cnt[len(rf.log)-1]=1 // here, you must +1, or, testPersist1, cmd14 will fail

	rf.mu.Unlock()

	// go func() {
		// DPrintf("Leader %v sending cmd %v to rf.applyChan\n", rf.me, command)
		// rf.requestChan <- command
	// }()

	// return immediately
	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//

func (rf *Raft) closeChan(channel chan int) {
	done := false
	for !done {
		select {
		case <- channel:
				done = false
		case <-time.After(10 * time.Millisecond): // 10ms waite
				done = true
		}
	}
	rf.closeChanChan <- 1
	close(channel)
	DPrintf("server %v close channel %v\n", rf.me, channel)
}

func (rf *Raft) KillImpl() {

	// Your code here, if desired.
	// go rf.closeChan(rf.voteCountChan)
	// go rf.closeChan(rf.toLeaderChan)
	// go rf.closeChan(rf.toFollowerChan)
	go rf.closeChan(rf.stayFollowerChan)

	count := 0
	for {
		if _, ok := <- rf.closeChanChan; ok {
			count++
		}
		if count == 1 { // fixme
			return
		}
	}
}

func (rf *Raft) Kill() {

	rf.mu.Lock()
	// DPrintf("Kill: %v save persister\n", rf.me)
	// rf.persist()
	// DPrintf("Kill: %v persister.raftdata: %v\n", rf.me, rf.persister.raftstate)
	rf.isDead = true
	rf.mu.Unlock()

	// go func() {
	rf.killSigChan <- 1
	// }()
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
//

func (rf *Raft) AsFollower() {
	DPrintf("server: %v as follower\n", rf.me)
	for rf.role == 0 {
		select {
		case <- rf.killSigChan:
			DPrintf("follower %v receive kill signal\n", rf.me)
			atomic.StoreInt32(&(rf.role), -1)
			rf.KillImpl()
			return
		case term := <-rf.stayFollowerChan:
			if rf.currentTerm == term {
				DPrintf("Follower %v remains follower\n", rf.me)
			}
		case <- time.After(time.Duration(300 + rand.Intn(200)) * time.Millisecond): // 300-400ms as election timeout
			DPrintf("server: %v timeout to candidate\n", rf.me)
			rf.mu.Lock()
			if rf.role == 0 {
			// time.Sleep(time.Duration(rand.Intn(50)) time.Millisecond)
				rf.role = 1
				go rf.AsCandidate()
			}
			rf.mu.Unlock()
			return
		} // handle request vote
	}
}

func (rf *Raft) CandidateCallForVotes() {
	// var mu sync.Mutex
	voteCount := int32(1) // start as 1, cause vote for itself
	majority := int32(len(rf.peers)/2 + 1)

	/* shared info across serveral rpc */
	rf.mu.Lock()
	Term := rf.currentTerm
	lastLogIndex := len(rf.log) - 1
	var lastLogTerm int
	if lastLogIndex < 0 {
			lastLogTerm = -1
	} else {
			lastLogTerm = rf.log[lastLogIndex].Term
	}
	if rf.role != 1 {
		return
	}
	rf.mu.Unlock()
	args := RequestVoteArgs{Term: Term, CandidateId: rf.me, LastLogIndex: lastLogIndex, LastLogTerm: lastLogTerm} // fixme

	// LOOPPEER:
	for i, _ := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(index int) {
			reply := &RequestVoteReply{}
			// rf.mu.Lock()
			// if atomic.LoadInt32(&(rf.role)) != 1 {
				// return
			// }
			// rf.mu.Unlock()
			ok := rf.sendRequestVote(index, args, reply)
			DPrintf("CandidateCallForVotes: %v to %v, Term:%v\n", rf.me, index, args.Term)
			if ok {
				if reply.VoteGranted {
					atomic.AddInt32(&voteCount, 1)
					DPrintf("%v voteCount: %v\n", rf.me, voteCount)
					if atomic.LoadInt32(&voteCount) >= majority {
						atomic.StoreInt32(&voteCount, 0)
						rf.mu.Lock()
						if rf.role == 1 { // only once
							 rf.role = 2
						   go rf.AsLeader()
						}
						rf.mu.Unlock()
						return
					}
				} else if (reply.Term > rf.currentTerm) { // receive greater Term
					atomic.StoreInt32(&voteCount, 0)
					rf.mu.Lock()
					if rf.role == 1 { // only once
						rf.role = 0
						rf.currentTerm = reply.Term
						rf.votedFor = -1
						rf.persist()
						go rf.AsFollower()
					}
					rf.mu.Unlock()
					return
				}
			} else {
					DPrintf("CandidateCallForVotes: %v to %v, Term:%v, rpc fail\n", rf.me, index, args.Term)
			}
			return // no matter whether `suc`, return
		} (i)
	}
	// if voteCount >= majority {
	// 	// atomic.StoreInt32(&voteCount, 0)
	// 	rf.mu.Lock()
	// 	if atomic.LoadInt32(&(rf.role)) == 1 { // only once
	// 		 atomic.StoreInt32(&(rf.role), 2)
	// 		 go rf.AsLeader()
	// 	}
	// 	rf.mu.Unlock()
	// 	return
	// }

}

func (rf *Raft) AsCandidate() {
	DPrintf("server: %v as candidate\n", rf.me)
	rf.mu.Lock()
	if rf.role != 1 { // in case of dealing RequestVoteRPC and become 'follower'
		return
	}
	rf.currentTerm++
	rf.votedFor = rf.me // vote for myself
	rf.persist()
	rf.mu.Unlock()
	// 1, go routine for votes counting
	// voteCount := 1 // start as 1, cause vote for itself
	// majority := len(rf.peers)/2 + 1
	// stop := int32(0)
	rf.CandidateCallForVotes()
	// 2, run as candidates

	for rf.role == 1 {
		select {
		case <- rf.killSigChan:
			DPrintf("candidate %v receive kill signal\n", rf.me)
			rf.KillImpl()
			atomic.StoreInt32(&(rf.role), -1)
			return
		case <- time.After(time.Duration(300+rand.Intn(200)) * time.Millisecond): // 300 - 400ms as election timeout
			rf.mu.Lock()
		 	if rf.role == 1 {
				DPrintf("candidate server: %v timeout, start another election\n", rf.me)
				rf.currentTerm += 1 // incr currentTerm
				rf.votedFor = rf.me // vote for myself
				rf.persist()
				// voteCount = 1
				go rf.CandidateCallForVotes()
			} // end if
			rf.mu.Unlock()
		} // end select
	} // end for
}

func (rf *Raft) LeaderSendAppendEntries() { // fixme
	rf.mu.Lock()
	Term := rf.currentTerm
	commitIndex := rf.commitIndex
	if rf.role != 2 { // make sure still as leader, so `Term` be valid
		return
	}
	rf.mu.Unlock()


	for i, _ := range rf.peers {
		if i == rf.me { // skip rf.me
			continue
		}
		// index := i
		go func(index int) {
			t := time.NewTicker(time.Millisecond * 50)
			// DPrintf("server: %v send append entries, receiver: %d\n", rf.me, index)
			for {
				select {
					case <- t.C:
						// doneChan <- true
						return

					default:
						rf.mu.Lock()
						prevLogIndex := rf.nextIndex[index] - 1
						var entries []LogEntry
						if rf.nextIndex[index] >= len(rf.log) {
							prevLogIndex = len(rf.log) - 1
							entries = nil
							DPrintf("heart beat: %v to %v\n", rf.me, index)
						} else {
							idx := rf.nextIndex[index]
							if idx < 0  {
								idx = 0
							}
							prevLogIndex = idx - 1
							entries = rf.log[idx:] // send only one entry every time
							DPrintf("append %v entries:%v, %v to %v\n", len(entries), entries, rf.me, index)
						}
						prevLogTerm := -1
						prevLogEntry := LogEntry{Term:-1, Command:nil}
						if prevLogIndex > -1 && len(rf.log) > prevLogIndex {
							prevLogTerm = rf.log[prevLogIndex].Term
							prevLogEntry = rf.log[prevLogIndex]
						}
						DPrintf("%v to %v, rf[%v].log:%v, prevLogIndex:%v, prevLogTerm: %v, prevLogEntry: %v\n", rf.me, index, rf.me, rf.log, prevLogIndex, prevLogTerm, prevLogEntry)
						rf.mu.Unlock()

						args := AppendEntriesArgs{Term: Term, LeaderId: rf.me, LeaderCommit: commitIndex, PrevLogIndex: prevLogIndex, PrevLogTerm: prevLogTerm, PrevLogEntry: prevLogEntry, Entries: entries} // fixme
						reply := &AppendEntriesReply{}

						ok := rf.sendAppendEntries(index, args, reply)
						DPrintf("%v to %v, get reply:%v\n", rf.me, index, ok)

						if !ok || reply == nil {
							return
						}

						rf.mu.Lock()
						if rf.role == 2 {
							if reply.Success {
								DPrintf("%v to %v, prevLogIndex: %v, prevLogTerm: %v entries: %v, reply Suc", rf.me, index, prevLogIndex, prevLogTerm, entries)
								if entries != nil { // not heart-beat
									// if atomic.LoadInt32(&(rf.role)) == 2 {
										// rf.mu.Lock() // ONLY COMMIT CURRENT TERM
										rf.nextIndex[index] = args.PrevLogIndex + len(args.Entries) + 1 // attention, +1 here
										rf.matchIndex[index] = rf.nextIndex[index] - 1
										DPrintf("Leader[%v]SendAppendEntries: args.PrevLogIndex=%v, len(args.Entries)=%v, rf.nextIndex[%v]=%v, rf.matchIndex[%v]=%v\n", rf.me, args.PrevLogIndex, len(args.Entries), index, rf.nextIndex[index], index, rf.matchIndex[index])
										// update commitIndex
										idx := rf.matchIndex[index]
										if rf.log[idx].Term == rf.currentTerm && idx > rf.commitIndex {
											_, ok := rf.index2cnt[idx]
											if !ok {
												rf.index2cnt[idx] = 1
												DPrintf("Leader[%v]SendAppendEntries: init rf.index2cnt[%v]=%v, entries:%v, prevLogIndex:%v, prevLogTerm:%v\n", rf.me, idx, rf.index2cnt[idx], entries, prevLogIndex, prevLogTerm)
											} else {
												rf.index2cnt[idx]++
												DPrintf("Leader[%v]SendAppendEntries: %v to %v ++rf.index2cnt[%v]=%v, entries:%v, prevLogIndex:%v, prevLogTerm:%v\n", rf.me, rf.me, index, idx, rf.index2cnt[idx], entries, prevLogIndex, prevLogTerm)
												if rf.index2cnt[idx] > len(rf.peers)/2 {
													lastCommitIdx := rf.commitIndex
													rf.commitIndex = idx
													rf.persist()
													// go func () {
													for i:=lastCommitIdx+1; i<rf.commitIndex+1 && i<len(rf.log); i++ {
														msg := ApplyMsg{Command: rf.log[i].Command, Index: i, UseSnapshot: false, Snapshot: nil}
														rf.applyChan <- msg
														DPrintf("LeaderSendAppendEntries: server:%v applyChan:%v\n", rf.me, msg)
													}
													// }()
													DPrintf("LeaderSendAppendEntries: Update CommitIdx to %v\n", idx)
												}
											}
										}
									// }
									// rf.mu.Unlock()
								}
								// doneChan <- true
								rf.mu.Unlock()
								return
							} else {
								DPrintf("%v to %v, prevLogIndex:%v, prevLogTerm: %v reply.Term:%v, rf.currentTerm:%v\n", rf.me, index, prevLogIndex, prevLogTerm, reply.Term, rf.currentTerm)
								// rf.mu.Lock()
								if reply.Term > rf.currentTerm { // become follower only if more `up to date` response
									DPrintf("%v to %v, prevLogIndex:%v, prevLogTerm: %v reply Fail: reply.Term:%v > rf.currentTerm:%v\n", rf.me, index, prevLogIndex, prevLogTerm, reply.Term, rf.currentTerm)
									if rf.role == 2  { // only become follower once
										rf.role = 0
										rf.currentTerm = reply.Term
										rf.votedFor = -1
										rf.persist()
										go rf.AsFollower()
										// doneChan <- true
									}
									rf.mu.Unlock()
									return
								}
								// rf.mu.Lock()
								// if rf.nextIndex[index] > 0 { // retry again
									// DPrintf("%v to %v, prevLogIndex:%v, prevLogTerm: %v reply.Term:%v, rf.currentTerm:%v\n", rf.me, index, prevLogIndex, prevLogTerm, reply.Term. rf.currentTerm)
									rf.nextIndex[index] = reply.NextIndex
									DPrintf("%v to %v, --rf.nextIndex[%v]: %v\n", rf.me, index, index, rf.nextIndex[index])
								// }
								rf.mu.Unlock()
								// return
							}
						} else { // retry again
							DPrintf("%v to %v, rpc fail\n", rf.me, index)
							// doneChan <- true
							rf.mu.Unlock()
							return // waite for next heartbeat
						}
						// rf.mu.Unlock()
					}// end select
				}
		}(i)
	}
}

func (rf *Raft) AsLeader() {
	DPrintf("server: %v as leader\n", rf.me)
	rf.mu.Lock()
	/* init nextIdx & matchIdx */
	for i, _ := range rf.peers {
		rf.nextIndex[i] = len(rf.log)
		rf.matchIndex[i] = -1 // fixme cong -1 kaishi
	}
	rf.mu.Unlock()
	DPrintf("leader:%v init done\n", rf.me)
	// stop := int32(0)
	rf.LeaderSendAppendEntries()
	for rf.role == 2 {
		select {
		case <- rf.killSigChan:
			DPrintf("leader select %v: receive kill signal\n", rf.me)
			atomic.StoreInt32(&(rf.role), -1)
			rf.KillImpl()
			return
		// case term := <-rf.toFollowerChan: // to follower
		case <-time.After(time.Duration(50 * len(rf.peers)) * time.Millisecond): // 20 - 30ms as election timeout
			rf.mu.Lock()
			if rf.role == 2 {
				DPrintf("leader select %v: timeout and send out heartbeat\n", rf.me)
				go rf.LeaderSendAppendEntries()
			}
			rf.mu.Unlock()
		}
	}
}

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	rf.applyChan = applyCh // fixme, a more compact way

	N := len(rf.peers)

	rf.log = make([]LogEntry, 0) // slice
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.commitIndex = -1
	rf.lastApplied = -1

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	DPrintf("Make.readPersist: rf[%v].commitIndex: %v\n", rf.me, rf.commitIndex)

	rf.isDead = false

	rf.nextIndex = make([]int, N)
	rf.matchIndex = make([]int, N)

	// rf.voteCountChan = make(chan int, N)
	// rf.toLeaderChan = make(chan int, 1)
	// rf.toFollowerChan = make(chan int, 1)
	rf.stayFollowerChan = make(chan int, 1)

	rf.index2cnt = make(map[int] int, 10)

	// rf.requestChan = make(chan interface{})

	rf.killSigChan = make(chan int, 1)
	rf.closeChanChan = make(chan int, 1)

	// initiall all r followers
	// go func() {
		time.Sleep(time.Duration(rand.Intn(10)) * time.Millisecond) // different init time
		go rf.AsFollower()
	// }()

	return rf
}
