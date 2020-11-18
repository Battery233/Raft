package raft

//
// API
// ===
//
// rf = NewPeer(...)
//   Create a new Raft server.
//
// rf.PutCommand(command interface{}) (index, term, isleader)
//   PutCommand agreement on a new log entry
//
// rf.GetState() (me, term, isLeader)
//   Ask a Raft peer for current id ("me"), its current term, and whether it thinks it
//   is a leader
//
// ApplyCommand
//   Each time a new entry is committed to the log, each Raft peer
//   should send an ApplyCommand to the service (e.g. tester) on the
//   same server, via the applyCh channel passed to NewPeer()
//

import (
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"sync"
	"time"

	"github.com/cmu440/rpc"
)

//
// Raft struct
// ===========
//
// A Go object implementing a single Raft peer
//
type Raft struct {
	mux sync.Mutex // Lock to protect shared access to this peer's state

	peers []*rpc.ClientEnd // RPC end points of all peers
	me    int              // this peer's index into peers[]

	//Persistent state on all servers:
	peerType    int        // the type of the server (leader, follower, candidate)
	currentTerm int        //latest term server has seen (initialized to 0 on first boot, increases monotonically)
	votedFor    int        //candidateId that received vote in current term (or null if none)
	logEntries  []logEntry // a slice to store logs

	//Volatile state on all servers:
	commitIndex int //index of highest log entry known to be committed (initialized to 0, increases monotonically)
	lastApplied int //index of highest log entry applied to state machine (initialized to 0, increases monotonically)

	//Volatile state on leaders:
	nextIndex  []int //for each server, index of the next log entry	to send to that server (initialized to leader last log index + 1)
	matchIndex []int //for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)

	//utils
	applyChan                chan ApplyCommand        //channel which is used to send commands to the state machine
	stopSignalChan           chan struct{}            // channel for closing down the server
	resetElectionTimeoutChan chan struct{}            //channel for resetting election timer
	requestVoteResultChan    chan *RequestVoteReply   //channel for sending voting results
	appendEntriesResultChan  chan *AppendEntriesReply //channel for sending log appending results

	logger *log.Logger //  a separate logger per peer.
}

// Set to false to disable debug logs completely
// Make sure to set kEnableDebugLogs to false before submitting
const kEnableDebugLogs = false

// Set to true to log to stdout instead of file
const kLogToStdout = true

// Change this to output logs to a different directory
const kLogOutputDir = "./raftlogs/"

// const values for representing server types
const (
	Follower = iota
	Candidate
	Leader
)

// const values for timeout settings
const (
	HeartbeatTime   = 125
	ElectionMinTime = 250
	ElectionMaxTime = 400
)

//
// ApplyCommand
// ========
//
// As each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyCommand to the service (or
// tester) on the same server, via the applyCh passed to NewPeer()
//
type ApplyCommand struct {
	Index   int
	Command interface{}
}

//
// logEntry
// ========
//
// A single log record with the Term of the log and Command
//
type logEntry struct {
	Term    int
	Command interface{}
}

//
// RequestVoteArgs
// ===============
//
// RequestVote RPC arguments structure
//
type RequestVoteArgs struct {
	Term         int // candidate’s term
	CandidateId  int // candidate requesting vote
	LastLogIndex int // index of candidate’s last log entry
	LastLogTerm  int //term of candidate’s last log entry
}

//
// RequestVoteReply
// ================
//
// RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

//
// AppendEntriesArgs
// ================
//
// AppendEntriesArgs RPC arguments structure.
//
type AppendEntriesArgs struct {
	Term         int        //leader’s term
	LeaderId     int        //so follower can redirect clients
	PrevLogIndex int        //index of log entry immediately preceding new ones
	PrevLogTerm  int        //term of prevLogIndex entry
	Entries      []logEntry // log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int        // leader’s commitIndex
}

//
// AppendEntriesArgs
// ================
//
// AppendEntriesArgs RPC reply structure.
//
type AppendEntriesReply struct {
	Id      int  //the id of the peer
	Index   int  // the last index of those appended logs
	Term    int  // currentTerm, for leader to update itself
	Success bool // true if follower contained entry matching prevLogIndex and prevLogTerm
}

//
// GetState()
// ==========
//
// Return the id of the server:"me", current term and whether this peer
// believes it is the leader
//
func (rf *Raft) GetState() (int, int, bool) {
	rf.mux.Lock()
	defer rf.mux.Unlock()
	return rf.me, rf.currentTerm, rf.peerType == Leader
}

//
// RequestVote
// ===========
//
// RequestVote RPC handler
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mux.Lock()
	voteGranted := false
	resetElectionTimer := false

	if args.Term > rf.currentTerm { //update term
		rf.logger.Printf("RequestVote: Peer %v term updated from %v to %v. Previous type: %v\n", rf.me, rf.currentTerm, args.Term, rf.peerType)
		rf.peerType = Follower
		rf.currentTerm = args.Term
		rf.votedFor = -1
		resetElectionTimer = true
	}

	// voting process
	if args.Term == rf.currentTerm &&
		(rf.votedFor == -1 || rf.votedFor == args.CandidateId) {

		lastLogIndex := len(rf.logEntries) - 1
		resetElectionTimer = true

		//vote yes if the log is up to date
		if args.LastLogTerm > rf.logEntries[lastLogIndex].Term ||
			(args.LastLogTerm == rf.logEntries[lastLogIndex].Term && args.LastLogIndex >= lastLogIndex) {
			voteGranted = true
			rf.votedFor = args.CandidateId
		} else {
			rf.logger.Printf("RequestVote: Peer %v votes no: logs are outdated\n", rf.me)
		}
	} else {
		rf.logger.Printf("RequestVote: Peer %v votes no: already voted in the current term or term mismatch", rf.me)
	}

	// generate voting reply message
	reply.Term = rf.currentTerm
	reply.VoteGranted = voteGranted
	rf.logger.Printf("RequestVote: Peer %v votes for peer %v: %v,  args:%v, me:%v\n", rf.me, args.CandidateId, voteGranted, args.Term, rf.currentTerm)
	rf.mux.Unlock()

	if resetElectionTimer { // reset election timer
		rf.resetElectionTimeoutChan <- struct{}{}
	}
}

//
// sendRequestVote
// ===============
//
// Example code to send a RequestVote RPC to a server
//
// server int -- index of the target server in
// rf.peers[]
//
// args *RequestVoteArgs -- RPC arguments in args
//
// reply *RequestVoteReply -- RPC reply
//
// The types of args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers)
//
// The rpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost
//
// Call() sends a request and waits for a reply
//
// If a reply arrives within a timeout interval, Call() returns true;
// otherwise Call() returns false
//
// Thus Call() may not return for a while
//
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply
//
// Call() is guaranteed to return (perhaps after a delay)
// *except* if the handler function on the server side does not return
//
// Thus there
// is no need to implement your own timeouts around Call()
//
// Please look at the comments and documentation in ../rpc/rpc.go
// for more details
//
// If you are having trouble getting RPC to work, check that you have
// capitalized all field names in the struct passed over RPC, and
// that the caller passes the address of the reply struct with "&",
// not the struct itself
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	if ok {
		rf.requestVoteResultChan <- reply
	}
	return ok
}

//
// AppendEntries
// ================
//
// RPC function to solve log appending requests or heartbeats.
//
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mux.Lock()
	resetElectionTimeout := false

	if args.Term > rf.currentTerm { // if the term is outdated, update it and step down
		rf.logger.Printf("AppendEntries: Peer %v term updated from %v to %v. Previous type: %v\n", rf.me, rf.currentTerm, args.Term, rf.peerType)
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.peerType = Follower
	}

	if args.Term < rf.currentTerm { // reject outdated requests
		reply.Success = false
		rf.logger.Printf("AppendEntries from Peer %v with term %v. (peer %v at term %v)!\n", args.LeaderId, args.Term, rf.me, rf.currentTerm)
	} else if args.PrevLogIndex > len(rf.logEntries)-1 { // missing logs
		reply.Success = false
		resetElectionTimeout = true
		rf.logger.Printf("AppendEntries: Peer %v at term %v. Append false. PrevLogIndex %v is greater than logEntries length \n", rf.me, rf.currentTerm, args.PrevLogIndex)
	} else if rf.logEntries[args.PrevLogIndex].Term != args.PrevLogTerm { // log does not match
		rf.logEntries = rf.logEntries[:args.PrevLogIndex]
		reply.Success = false
		resetElectionTimeout = true
		rf.logger.Printf("AppendEntries: Peer %v at term %v. Append false. Log conflict at PrevLogIndex %v\n", rf.me, rf.currentTerm, args.PrevLogIndex)
	} else if args.Entries == nil { // this is a heartbeat message
		reply.Success = true
		rf.peerType = Follower
		resetElectionTimeout = true
	} else { // legit request, append the log
		rf.logEntries = append(rf.logEntries[:args.PrevLogIndex+1], args.Entries...)
		reply.Success = true
		resetElectionTimeout = true
		rf.logger.Printf("AppendEntries: Peer %v at term %v. append %v more entries. Total valid entry lenth now: %v\n", rf.me, rf.currentTerm, len(args.Entries), len(rf.logEntries)-1)
		for _, v := range args.Entries {
			rf.logger.Printf("Content of the entry appended-> Term:%v, %v\n", v.Term, v.Command)
		}
	}

	reply.Term = rf.currentTerm

	// update the commit information according to the commitIndex from the Leader
	if args.LeaderCommit > rf.commitIndex && reply.Success {
		if args.LeaderCommit <= len(rf.logEntries)-1 {
			rf.commitIndex = args.LeaderCommit
			rf.logger.Printf("AppendEntries: Peer %v at term %v. Update commmitIndex to LeaderCommit:%v\n", rf.me, rf.currentTerm, args.LeaderCommit)
		} else {
			rf.commitIndex = len(rf.logEntries) - 1
			rf.logger.Printf("AppendEntries: Peer %v at term %v. Update commmitIndex to Length of entries -1 :%v\n", rf.me, rf.currentTerm, rf.commitIndex)
		}
	}

	// commit to state machines
	for rf.lastApplied < rf.commitIndex {
		rf.lastApplied++
		rf.applyChan <- ApplyCommand{
			Index:   rf.lastApplied,
			Command: rf.logEntries[rf.lastApplied].Command,
		}
		rf.logger.Printf("AppendEntries: Peer %v using applyChan for log index: %v, command: %v", rf.me, rf.lastApplied, rf.logEntries[rf.lastApplied].Command)
	}

	rf.mux.Unlock()

	if resetElectionTimeout {
		rf.resetElectionTimeoutChan <- struct{}{}
	}
}

//
// sendAppendEntries
// ===============
//
// code to send a AppendEntries RPC to a server
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if ok {
		rf.appendEntriesResultChan <- reply
	}
	return ok
}

//
// PutCommand
// =====
//
// The service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log
//
// If this server is not the leader, return false
//
// Otherwise start the agreement and return immediately
//
// There is no guarantee that this command will ever be committed to
// the Raft log, since the leader may fail or lose an election
//
// The first return value is the index that the command will appear at
// if it is ever committed
//
// The second return value is the current term
//
// The third return value is true if this server believes it is
// the leader
//
func (rf *Raft) PutCommand(command interface{}) (int, int, bool) {
	rf.mux.Lock()
	defer rf.mux.Unlock()

	term := rf.commitIndex
	isLeader := rf.peerType == Leader

	if isLeader { // only the leader can put new commands
		rf.matchIndex[rf.me] = len(rf.logEntries)
		rf.nextIndex[rf.me] = rf.matchIndex[rf.me] + 1
		// add the command to the log slice
		rf.logEntries = append(rf.logEntries, logEntry{
			Command: command,
			Term:    rf.currentTerm,
		})
		rf.logger.Printf("PutCommand: Peer %v at term %v. Index in log entries: %v, Command %v", rf.me, rf.currentTerm, len(rf.logEntries)-1, command)
	}
	return len(rf.logEntries) - 1, term, isLeader
}

//
// Stop
// ====
//
// The tester calls Stop() when a Raft instance will not
// be needed again
//
// You are not required to do anything
// in Stop(), but it might be convenient to (for example)
// turn off debug output from this instance
//
func (rf *Raft) Stop() {
	rf.stopSignalChan <- struct{}{}
}

//
// NewPeer
// ====
//
// The service or tester wants to create a Raft server
//
// The port numbers of all the Raft servers (including this one)
// are in peers[]
//
// This server's port is peers[me]
//
// All the servers' peers[] arrays have the same order
//
// applyCh
// =======
//
// applyCh is a channel on which the tester or service expects
// Raft to send ApplyCommand messages. You can assume the channel
// is consumed in a timely manner.
//
// NewPeer() must return quickly, so it should start Goroutines
// for any long-running work
func NewPeer(peers []*rpc.ClientEnd, me int, applyCh chan ApplyCommand) *Raft {
	rf := &Raft{
		peers:                    peers,
		me:                       me,
		applyChan:                applyCh,
		peerType:                 Follower,
		currentTerm:              -1,
		votedFor:                 -1,
		logEntries:               make([]logEntry, 1),
		commitIndex:              0,
		lastApplied:              0,
		nextIndex:                make([]int, len(peers)),
		matchIndex:               make([]int, len(peers)),
		stopSignalChan:           make(chan struct{}),
		resetElectionTimeoutChan: make(chan struct{}),
		requestVoteResultChan:    make(chan *RequestVoteReply),
		appendEntriesResultChan:  make(chan *AppendEntriesReply),
	}

	if kEnableDebugLogs {
		peerName := peers[me].String()
		logPrefix := fmt.Sprintf("%s ", peerName)
		if kLogToStdout {
			rf.logger = log.New(os.Stdout, peerName, log.Lmicroseconds|log.Lshortfile)
		} else {
			err := os.MkdirAll(kLogOutputDir, os.ModePerm)
			if err != nil {
				panic(err.Error())
			}
			logOutputFile, err := os.OpenFile(fmt.Sprintf("%s/%s.txt", kLogOutputDir, logPrefix), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0755)
			if err != nil {
				panic(err.Error())
			}
			rf.logger = log.New(logOutputFile, logPrefix, log.Lmicroseconds|log.Lshortfile)
		}
		rf.logger.Println("logger initialized")
	} else {
		rf.logger = log.New(ioutil.Discard, "", 0)
	}

	// setup the first padding log record
	rf.logEntries[0] = logEntry{
		Command: nil,
		Term:    -1,
	}

	go rf.mainRoutine()

	return rf
}

//
// mainRoutine
// ===============
//
// goroutine for handling requests and timeout events
//
func (rf *Raft) mainRoutine() {
	// the server starts as a Followr
	electionTimeoutTimer := randomElectionTimeoutTimer()
	heartBeatTimer := time.NewTimer(time.Millisecond * time.Duration(HeartbeatTime))
	heartBeatTimer.Stop()
	// vars for majority voting
	currentVotes := 0
	majority := len(rf.peers)/2 + 1

	for {
		select {
		case <-rf.stopSignalChan: // stop the server
			rf.logger.Printf("Peer %v stop() called!\n", rf.me)
			return

		case <-rf.resetElectionTimeoutChan:
			heartBeatTimer.Stop()
			electionTimeoutTimer = randomElectionTimeoutTimer()

		case <-electionTimeoutTimer.C: // start a new election as a candidate
			currentVotes = 0
			electionTimeoutTimer = randomElectionTimeoutTimer()

			rf.mux.Lock()
			me := rf.me
			rf.logger.Printf("Peer %v starts an new election\n", rf.me)
			rf.peerType = Candidate
			rf.currentTerm++
			rf.votedFor = rf.me //vote for itself
			currentVotes++
			requestVoteArgs := &RequestVoteArgs{
				Term:         rf.currentTerm,
				CandidateId:  rf.me,
				LastLogIndex: len(rf.logEntries) - 1,
				LastLogTerm:  rf.logEntries[len(rf.logEntries)-1].Term,
			}
			rf.mux.Unlock()
			for i := range rf.peers { // send voting requests
				if i != me {
					go rf.sendRequestVote(i, requestVoteArgs, &RequestVoteReply{})
				}
			}

		case reply := <-rf.appendEntriesResultChan: //handling appendEntries results
			rf.mux.Lock()
			if reply.Success {
				rf.matchIndex[reply.Id] = reply.Index
				rf.nextIndex[reply.Id] = reply.Index + 1
				//update commitIndex and apply committed logs status to state machine
				rf.updateCommitIndex(reply.Index, majority)
				for rf.lastApplied < rf.commitIndex {
					rf.lastApplied++
					rf.applyChan <- ApplyCommand{
						Index:   rf.lastApplied,
						Command: rf.logEntries[rf.lastApplied].Command,
					}
					rf.logger.Printf("appendEntriesResultChan: Leader Peer %v using applyChan for log index: %v, command: %v", rf.me, rf.lastApplied, rf.logEntries[rf.lastApplied].Command)
				}
			} else if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.votedFor = -1
				rf.logger.Printf("appendEntriesResultChan: Peer %v steps down from %v to follower\n", rf.me, rf.peerType)
				//step down as a follower
				rf.peerType = Follower
				heartBeatTimer.Stop()
				electionTimeoutTimer = randomElectionTimeoutTimer()
			} else { //log conflict, step back one log
				rf.nextIndex[reply.Id]--
				rf.logger.Printf("appendEntriesResultChan: Log confilct: Peer %v nextIndex--. Now value is %v\n", reply.Id, rf.nextIndex[reply.Id])
			}
			rf.mux.Unlock()

		case reply := <-rf.requestVoteResultChan: //handling leader election voting results
			rf.mux.Lock()
			if rf.peerType == Candidate {
				if reply.VoteGranted == true {
					currentVotes++
					if currentVotes >= majority {
						rf.logger.Printf("Peer %v just elected as the leader!\n", rf.me)
						// becomes a leader and send heartBeat right after it!
						heartBeatTimer.Reset(time.Millisecond * time.Duration(0))
						electionTimeoutTimer.Stop()
						rf.peerType = Leader
						for i := range rf.nextIndex {
							rf.nextIndex[i] = len(rf.logEntries)
							rf.matchIndex[i] = 0
						}
						rf.matchIndex[rf.me] = len(rf.logEntries) - 1
					}
				} else if reply.Term > rf.currentTerm { //step down
					rf.currentTerm = reply.Term
					rf.votedFor = -1
					rf.peerType = Follower
					rf.logger.Printf("requestVoteResultChan: Peer %v steps down from Candidate to follower\n", rf.me)
					electionTimeoutTimer = randomElectionTimeoutTimer()
				}
			}
			rf.mux.Unlock()

		case <-heartBeatTimer.C: // heartbeat event
			heartBeatTimer.Reset(time.Millisecond * time.Duration(HeartbeatTime))
			rf.mux.Lock()
			if rf.peerType != Leader {
				rf.mux.Unlock()
				break
			}
			argsArray := make([]AppendEntriesArgs, len(rf.peers))
			replyArray := make([]AppendEntriesReply, len(rf.peers))
			me := rf.me
			lastLogIndex := len(rf.logEntries) - 1

			for i := range rf.peers {
				var entries []logEntry = nil
				if lastLogIndex >= rf.nextIndex[i] {
					entries = rf.logEntries[rf.nextIndex[i]:] // setup entries to send
				}
				argsArray[i] = AppendEntriesArgs{
					Term:         rf.currentTerm,
					LeaderId:     rf.me,
					PrevLogIndex: rf.nextIndex[i] - 1,
					PrevLogTerm:  rf.logEntries[rf.nextIndex[i]-1].Term,
					Entries:      entries,
					LeaderCommit: rf.commitIndex,
				}
				replyArray[i] = AppendEntriesReply{
					Id:    i,
					Index: lastLogIndex,
				}
			}
			rf.mux.Unlock()

			for i := 0; i < len(rf.peers); i++ {
				if i != me {
					go rf.sendAppendEntries(i, &argsArray[i], &replyArray[i])
				}
			}
		}
	}
}

//
// updateCommitIndex
// ===============
//
// helper function to test if the commits reached a majority vote situation and update the commitIndex value
//
func (rf *Raft) updateCommitIndex(upperBound int, majority int) {
	for N := rf.commitIndex + 1; N <= upperBound; N++ {
		count := 0
		for _, v := range rf.matchIndex {
			if v >= N {
				count++
			}
			if count >= majority {
				if rf.currentTerm == rf.logEntries[N].Term {
					rf.logger.Printf("Leader: majority agree achieved at log id = %v, term = %v \n", N, rf.currentTerm)
					rf.commitIndex = N
					break
				} else {
					rf.logger.Printf("Leader: majority agree achieved at log id = %v but term mismatch. Expect term = %v but get %v \n", N, rf.currentTerm, rf.logEntries[N].Term)
				}
			}
		}
		if count < majority {
			return
		}
	}
}

//
// randomElectionTimeoutTimer
// ===============
//
// helper function to generate a random timer for election timeout events
//
func randomElectionTimeoutTimer() *time.Timer {
	return time.NewTimer(time.Millisecond * time.Duration(ElectionMinTime+rand.Intn(ElectionMaxTime-ElectionMinTime)))
}
