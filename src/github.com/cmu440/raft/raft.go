//
// raft.go
// =======
// Write your code in this file
// We will use the original version of all other
// files for testing
//

package raft

//
// API
// ===
// This is an outline of the API that your raft implementation should
// expose.
//
// rf = NewPeer(...)
//   Create a new Raft server.
//
// rf.PutCommand(command interface{}) (index, term, isleader)
//   PutCommand agreement on a new log entry
//
// rf.GetState() (me, term, isLeader)
//   Ask a Raft peer for "me" (see line 58), its current term, and whether it thinks it
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

// Set to false to disable debug logs completely
// Make sure to set kEnableDebugLogs to false before submitting
const kEnableDebugLogs = false

// Set to true to log to stdout instead of file
const kLogToStdout = true

// Change this to output logs to a different directory
const kLogOutputDir = "./raftlogs/"

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

//todo set print to false
//todo check struct upper cases

//
// Raft struct
// ===========
//
// A Go object implementing a single Raft peer
//
type Raft struct {
	mux    sync.Mutex       // Lock to protect shared access to this peer's state
	peers  []*rpc.ClientEnd // RPC end points of all peers
	me     int              // this peer's index into peers[]
	logger *log.Logger      //  a separate logger per peer.

	applyChan chan ApplyCommand
	peerType  int
	//Persistent state on all servers:
	currentTerm int //latest term server has seen (initialized to 0 on first boot, increases monotonically)
	//todo update votedFor to -1 when a new term starts
	votedFor   int //candidateId that received vote in current term (or null if none)
	logEntries []logEntry

	//Volatile state on all servers:
	commitIndex int
	lastApplied int

	//Volatile state on leaders:
	//todo Reinitialized after election
	nextIndex  []int
	matchIndex []int

	//utils
	stopSignalChan           chan struct{}
	resetElectionTimeoutChan chan struct{}
	requestVoteResultChan    chan *RequestVoteReply
	appendEntriesResultChan  chan *AppendEntriesReply
}

type logEntry struct {
	Command interface{}
	Term    int
}

const (
	Follower = iota
	Candidate
	Leader
)

const (
	HeartbeatTime   = 125
	ElectionMinTime = 275
	ElectionMaxTime = 500
)

//
// GetState()
// ==========
//
// Return "me", current term and whether this peer
// believes it is the leader
//
func (rf *Raft) GetState() (int, int, bool) {
	rf.mux.Lock()
	defer rf.mux.Unlock()
	return rf.me, rf.currentTerm, rf.peerType == Leader
}

//
// RequestVoteArgs
// ===============
//
// Example RequestVote RPC arguments structure
//
type RequestVoteArgs struct {
	Term        int // candidate’s term
	CandidateId int // candidate requesting vote
	//todo check these two
	LastLogIndex int // index of candidate’s last log entry
	LastLogTerm  int //term of candidate’s last log entry
}

//
// RequestVoteReply
// ================
//
// Example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

type AppendEntriesArgs struct {
	Term         int        //leader’s term
	LeaderId     int        //so follower can redirect clients
	PrevLogIndex int        //index of log entry immediately preceding new ones
	PrevLogTerm  int        //term of prevLogIndex entry
	Entries      []logEntry // log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int        // leader’s commitIndex
}

type AppendEntriesReply struct {
	Term    int  // currentTerm, for leader to update itself
	Success bool // true if follower contained entry matching prevLogIndex and prevLogTerm
}

//todo implementation!

//
// RequestVote
// ===========
//
// Example RequestVote RPC handler
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// todo Your code here (2A, 2B)
	rf.mux.Lock()
	defer rf.mux.Unlock()

	voteGranted := false
	resetVoteFor := false
	if args.Term > rf.currentTerm {
		rf.peerType = Follower
		rf.logger.Printf("RequestVote: Peer %v term updated from %v to %v\n", rf.me, rf.currentTerm, args.Term)
		rf.currentTerm = args.Term
		//todo check if need to reset voted here?
		resetVoteFor = true
		//todo is this timer reset correct?
		rf.resetElectionTimeoutChan <- struct{}{}
	}

	if args.Term == rf.currentTerm && (rf.votedFor == -1 || rf.votedFor == args.CandidateId) {
		lastLogIndex := len(rf.logEntries) - 1
		if args.LastLogTerm > rf.logEntries[lastLogIndex].Term ||
			(args.LastLogTerm == rf.logEntries[lastLogIndex].Term && args.LastLogIndex >= lastLogIndex) {
			voteGranted = true
			resetVoteFor = false
			rf.votedFor = args.CandidateId
			//todo check the correctness of this code
			rf.resetElectionTimeoutChan <- struct{}{}
		} else {
			rf.logger.Printf("RequestVote: Peer %v else 1\n", rf.me)
		}
	} else {
		rf.logger.Printf("RequestVote: Peer %v else 2\n", rf.me)
	}

	if resetVoteFor {
		rf.votedFor = -1
	}

	reply.Term = rf.currentTerm
	reply.VoteGranted = voteGranted
	rf.logger.Printf("RequestVote: Peer %v votes for peer %v: %v,  args:%v, me:%v\n", rf.me, args.CandidateId, voteGranted, args.Term, rf.currentTerm)
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

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mux.Lock()
	defer rf.mux.Unlock()
	rf.logger.Printf("heartbeat received from Peer %v with term %v. (peer %v at term %v)!\n", args.LeaderId, args.Term,rf.me, rf.currentTerm)
	if args.Term < rf.currentTerm {
		//todo if the receiver is a previous isolated follower with a high term value?
		//todo after isolation, the isolated follower will have a very high term, but will not be the next leader
		reply.Success = false
		//todo reset timer?
	} else {
		rf.currentTerm = args.Term
		//todo if log mismatch
		reply.Success = true
		rf.peerType = Follower
		rf.resetElectionTimeoutChan <- struct{}{}
	}
	reply.Term = rf.currentTerm
}

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
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B)

	return index, term, isLeader
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
		currentTerm:              0,
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

	rf.logEntries[0] = logEntry{
		Command: nil,
		Term:    -1,
	}

	go rf.mainRoutine()

	return rf
}

func (rf *Raft) mainRoutine() {
	electionTimeoutTimer := randomElectionTimeoutTimer()
	heartBeatTicker := time.NewTicker(time.Millisecond * time.Duration(HeartbeatTime))
	heartBeatTicker.Stop()
	currentVotes := 0
	majority := len(rf.peers)/2 + 1
	for {
		select {
		case <-rf.stopSignalChan:
			rf.logger.Printf("Peer %v stop() called!\n", rf.me)
			return
		case <-rf.resetElectionTimeoutChan:
			heartBeatTicker.Stop()
			electionTimeoutTimer = randomElectionTimeoutTimer()
		case <-electionTimeoutTimer.C:
			currentVotes = 0
			electionTimeoutTimer = randomElectionTimeoutTimer()
			go rf.startElection()
		case reply := <-rf.appendEntriesResultChan:
			rf.mux.Lock()
			if reply.Success {
				//todo
			} else {
				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.votedFor = -1
					if rf.peerType != Follower {
						rf.peerType = Follower
						heartBeatTicker.Stop()
					}
					electionTimeoutTimer = randomElectionTimeoutTimer()
				}
			}
			rf.mux.Unlock()
		case reply := <-rf.requestVoteResultChan:
			rf.mux.Lock()
			if rf.peerType == Candidate {
				if reply.VoteGranted == true {
					currentVotes++
					if currentVotes >= majority {
						rf.logger.Printf("Peer %v just elected as the leader!\n", rf.me)
						heartBeatTicker.Reset(time.Millisecond * time.Duration(HeartbeatTime))
						//todo restart it when the leader steps down!
						electionTimeoutTimer.Stop()
						rf.peerType = Leader
						for i := range rf.nextIndex {
							rf.nextIndex[i] = len(rf.logEntries)
							rf.matchIndex[i] = 0
						}
						rf.matchIndex[rf.me] = len(rf.logEntries) - 1
					}
				} else if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.votedFor = -1
					rf.peerType = Follower
					heartBeatTicker.Stop()
				}
			}
			rf.mux.Unlock()
		case <-heartBeatTicker.C:
			//todo add actual entries later
			rf.mux.Lock()
			rf.logger.Printf("Time to send heartbeat! Am I a leader?:%v, term:%v \n", rf.peerType==Leader, rf.currentTerm)
			for i := range rf.peers {
				args := &AppendEntriesArgs{
					Term:         rf.currentTerm,
					LeaderId:     rf.me,
					PrevLogIndex: rf.nextIndex[i] - 1,
					PrevLogTerm:  rf.logEntries[rf.nextIndex[i]-1].Term,
					Entries:      nil,
					LeaderCommit: rf.commitIndex,
				}
				reply := &AppendEntriesReply{}

				if i != rf.me {
					rf.mux.Unlock()
					go rf.sendAppendEntries(i, args, reply)
					rf.mux.Lock()
				}
			}
			rf.mux.Unlock()
		}
	}
	//todo unlock check?!!!
}

func (rf *Raft) startElection() {
	//todo check if this lock is correct
	rf.mux.Lock()
	rf.logger.Printf("Peer %v starts an new election\n", rf.me)
	rf.peerType = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	requestVoteArgs := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: len(rf.logEntries) - 1,
		LastLogTerm:  rf.logEntries[len(rf.logEntries)-1].Term,
	}
	rf.mux.Unlock()
	for i := range rf.peers {
		reply := &RequestVoteReply{}
		go rf.sendRequestVote(i, requestVoteArgs, reply) //request rpc, unlock here
	}
}

func randomElectionTimeoutTimer() *time.Timer {
	return time.NewTimer(time.Millisecond * time.Duration(ElectionMinTime+rand.Intn(ElectionMaxTime-ElectionMinTime)))
}
