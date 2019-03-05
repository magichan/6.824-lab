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
	"sync"
	"time"
)
import "6824_2018/labrpc"

// import "bytes"
// import "labgob"



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

const (
	FOLLOWER = iota
	CANDIDATE
	LEADER
)
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
	currentTerm int // default 0
	votedFor    int // -1 is noting
	state       int // 0 is follower | 1 is candidate | 2 is leader
	timeout     int

	heartbeat       chan HeatbeatMeta
	cancelSelection chan struct{}
	startUpCancelSelection chan struct{}
	becomeLeader    chan bool
}

func (rf *Raft) ToFollower(term int, votedFor int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.currentTerm = term
	rf.votedFor = votedFor
	rf.state = FOLLOWER
}
func (rf *Raft) ToCandidate() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.votedFor = rf.me
	rf.currentTerm = rf.currentTerm + 1
	rf.state = CANDIDATE
}
func (rf *Raft) ToLeader() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.votedFor = rf.me

	rf.state = LEADER
}


// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).

	term = rf.currentTerm
	if rf.state == LEADER {
		isleader = true
	} else {
		isleader = false
	}
	DPrintf("Server %d:Term:%d Status %d", rf.me, rf.currentTerm, rf.state)
	return term, isleader
}
func (rf *Raft) renewTimeout() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.timeout = randInt(200, 300)
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
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term        int
	CandidateId int
	//LastLogIndex int
	//LastLogTerm int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}
type AppendEntriesArgs struct {
	Term     int
	LeaderId int
}
type AppendEntriesReply struct {
	Term    int
	Success bool
}
type HeatbeatMeta struct {
	Term     int
	VotedFor int
	TmpFlag  string
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	DPrintf3("Server %d(%s,Term:%d) Accept Request Vote,args.Term:%d,args.CandidateId:%d", rf.me, rf.status(), rf.currentTerm, args.Term, args.CandidateId)
	// Your code here (2A, 2B).
	// 什么时候可以投，什么时候不可以投，
	// 投票后是否重置计时器。
	// 什么时候可以投
	// 		如果 VotedFor 为空或者就是 candidateId，并且候选人的日志至少和自己一样新，那么就投票给他（5.2 节，5.4 节）
	//      并且发送 heartbeat
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.currentTerm < args.Term {
		DPrintf3("Server %d vote Candidate %d for term get behind", rf.me, args.CandidateId)
		reply.Term = args.Term
		reply.VoteGranted = true
		if rf.state == CANDIDATE {

			DPrintf5("Server %d(%s) try cancel startUp selection", rf.me,rf.status())
			rf.startUpCancelSelection <- struct{}{}
			DPrintf5("Server %d(%s) success  cancel startUp selection ", rf.me,rf.status())

		}
		rf.heartbeat <- HeatbeatMeta{Term: args.Term, VotedFor: args.CandidateId, TmpFlag: "RequestVote"}
		return
	} else if rf.currentTerm == args.Term && (rf.votedFor == -1 || rf.votedFor == args.CandidateId) {
		//if rf.currentTerm <= args.Term && (rf.votedFor == -1 || rf.votedFor == args.CandidateId) {
		DPrintf3("Server %d vote Candidate %d", rf.me, args.CandidateId)
		reply.Term = args.Term
		reply.VoteGranted = true
		rf.heartbeat <- HeatbeatMeta{Term: args.Term, VotedFor: args.CandidateId, TmpFlag: "RequestVote"}
		return
	} else if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	return
}
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	DPrintf("Server %d Accept Append Entry", rf.me)

	// 2A 没有日志功能
	// 如果为 HeatBeats 的功能
	//如果 Term < currentTerm 就返回 false （5.1 节）
	//获取这个心跳信号，意味这投票已经结束了，所有的接受该信号的人都已经变为 Leader
	// 	将这个 Raft 转换为 Leader 的 Follower，重置投票记录，如果是 Candidate 转换的话，需要取消这个 raft 端的
	//rf.mu.Lock()
	//defer  rf.mu.Unlock()
	if rf.currentTerm > args.Term {
		DPrintf("Server %d:Append Entry Deal Fail", rf.me)

		reply.Term = rf.currentTerm
		reply.Success = false
	} else {
		DPrintf("Server %d:Append Entry Deal Success, Arg:%v", rf.me, args)
		if rf.state == CANDIDATE {
			DPrintf5("Server %d(%s) try cancel startUp selection", rf.me,rf.status())
			rf.startUpCancelSelection <- struct{}{}
			DPrintf5("Server %d(%s) success  cancel startUp selection ", rf.me,rf.status())
		}
		rf.heartbeat <- HeatbeatMeta{Term: args.Term, VotedFor: -1, TmpFlag: "AppendEntries"}
		reply.Term = -1
		reply.Success = true
		rf.GetState()

	}

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
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).


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
func (rf *Raft) startUp() {

	timer := time.NewTimer(time.Duration(rf.timeout) * time.Millisecond)
	DPrintf5("Server %d(%s) Startup,Timeout:%d", rf.me, rf.status(), rf.timeout)
	for {
		select {
		case <-timer.C:
			if rf.state == LEADER {
				DPrintf4("Server %d(%s) make heat beats", rf.me, rf.status())
				go rf.makeHeatBeat()
				timer.Reset(150 * time.Millisecond)
			} else if rf.state == CANDIDATE {
				DPrintf4("Server %d(%s) make vote", rf.me, rf.status())
				go rf.makeRequestVote()

				select {
				case <-time.After(time.Duration(rf.timeout) * time.Millisecond):
					// 选举超时
					DPrintf3("Server %d(%s) Election Timeout", rf.me, rf.status())
					rf.cancelSelection <- struct{}{}

					//	rf.ToFollower(rf.currentTerm,-1)
					rf.renewTimeout()
					rf.ToCandidate()

					DPrintf3("Server %d turn to Follower and timeout:%d", rf.me, rf.timeout)
					timer.Reset(time.Duration(0) * time.Millisecond)
				case success := <-rf.becomeLeader:
					if success {
						DPrintf4("Election End, The server %d becomes leader ", rf.me)
						rf.ToLeader()
					} else {
						DPrintf3("Election End, The server %d fail become leader ", rf.me)
					}
					timer.Reset(time.Duration(0) * time.Millisecond)
				case <- rf.startUpCancelSelection:
					rf.cancelSelection <- struct{}{}
					// don't need reset timer , for rf.heartbeat is coming
				}
			} else { // FOLLOWER
				DPrintf4("Server %d Turn To Candidate", rf.me)
				rf.ToCandidate()
				timer.Reset(0)
			}
		case ht := <-rf.heartbeat:
			DPrintf4("Server %d(%s):Rest Timer for heartbeat(%s)", rf.me, rf.status(), ht.TmpFlag)
			if !timer.Stop() {
				<-timer.C
			}
			rf.ToFollower(ht.Term, ht.VotedFor)
			timer.Reset(time.Duration(rf.timeout) * time.Millisecond)
			//timer.Reset(time.Duration(0)* time.Millisecond)

		}
	}

	/*
		有两个定时器，一个是 心跳时间大于 150 毫秒 ，一个是等待心跳在 200 ~ 300 毫秒之间，等待选举超时，
		启动一个随即定时器， 100 ~ 200
		Headbeats 的 timeout 要小于 200 毫秒
		在 5 s 内选出新的 leader
		初始化：
		rf.timeout = randtime(200~300)
		timer := NewTime(rf.timeout) // 设置定时器
		for(){
			switch {
			case <- timeout(dddd);
				如果是 Leader 的话，
				发出心跳请求；
				如果是 Candidate
				1.发出投票请求，设置等待选举定时器，
				2. 等待投票结果 & 等待 和 heatbeast
				3.1. 如果成为 Leader，，并且重置计时器为 0
				3.2  如果选举超时，变为 Follower ,随机设置， 200 ~ 300
				如果是 Follower 的话，
				转换为 Candidate ，充值定时器为 0
			case <- heatbeats
				重置计数器
			}
		}
	*/
}
func (rf *Raft) makeHeatBeat() {
	replyChannel := make(chan *AppendEntriesReply, 10)
	//CancelStatistics := make(chan struct{})
	serverNumber := len(rf.peers)
	wg := sync.WaitGroup{}
	wg2 := sync.WaitGroup{}
	wg2.Add(1)

	for i := 0; i < serverNumber; i++ {
		if i != rf.me {

			args := &AppendEntriesArgs{Term: rf.currentTerm, LeaderId: rf.me}
			reply := &AppendEntriesReply{}
			wg.Add(1)
			go func(server int) {
				//	DPrintf5("Server %d(%s) make a heartbeat to Server %d Start, arg %v",rf.me,rf.status(),server,args)
				var ok bool
				callHook := make(chan struct{})

				go func() {
					ok = rf.peers[server].Call("Raft.AppendEntries", args, reply)
					callHook <- struct{}{}
				}()

				select {
				case <-time.After(200 * time.Millisecond):
					replyChannel <- nil
					wg.Done()
					//logrus.Warnf("Server %d(%s) call AppendEntries of Server %d Fail ,For timeout", rf.me, rf.status(), server)
				case <-callHook:
					if ok {
						replyChannel <- reply
					} else {
						DPrintf5("Server %d[%s] call AppendEntries of Server %d Fail ,For return fail", rf.me, rf.status(), server)
						replyChannel <- nil
					}
					wg.Done()
				}
				//	DPrintf5("Server %d(%s) make a heartbeat to Server %d End, return %v",rf.me,rf.status(),server,reply)
			}(i)
		}
	}

	heartbeatSuccess := 0
	heartbeatFail := 0
	netBroken := 0
	go func() {
		//  statistics Goroutine
		for {
			select {
			case reply, ok := <-replyChannel:
				if !ok {
					wg2.Done()
					return
				}
				if reply != nil {
					if reply.Success {
						heartbeatSuccess = heartbeatSuccess + 1
					} else {
						heartbeatFail = heartbeatFail + 1
					}

				} else {
					netBroken = netBroken + 1
				}
			}
		}
	}()

	wg.Wait()
	close(replyChannel)
	wg2.Wait()
	//CancelStatistics <- struct{}{} // close Election statistics Goroutine
	DPrintf3("Service %d heartbeat data: Success:%d ,fail:%d ,net broken:%d\n", rf.me, heartbeatSuccess, heartbeatFail, netBroken)
}

func (rf *Raft) makeRequestVote() {

	replyChannel := make(chan *RequestVoteReply, 10)
	//funcCancelSelect := make(chan struct{})
	serverNumber := len(rf.peers)
	wg := sync.WaitGroup{}
	wg2 := sync.WaitGroup{}
	wg2.Add(1)

	for i := 0; i < serverNumber; i++ {
		if i != rf.me {
			args := &RequestVoteArgs{Term: rf.currentTerm, CandidateId: rf.me}
			reply := &RequestVoteReply{}
			wg.Add(1)
			go func(server int) {
				//var ok bool
				//callHook := make(chan struct{})
				//go func() {
				//	ok = rf.peers[server].Call("Raft.RequestVote", args, reply)
				//	callHook <- struct{}{}
				//}()
				//
				//select {
				//case <-time.After(200 * time.Millisecond):
				//	wg.Done()
				//	replyChannel <- nil
				//	logrus.Warnf("Server %d(%s) call RequestVote of Server %d Fail ,For timeout", rf.me, rf.status(), server)
				//case <-callHook:
				//	if ok {
				//		replyChannel <- reply
				//	} else {
				//		DPrintf5("Server %d[%s] call RequestVote of Server %d Fail ,For return fail", rf.me, rf.status(), server)
				//		replyChannel <- nil
				//	}
				//	wg.Done()
				//}
				ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
				if ok {
					// TODO
					// 考虑这样一个问题，当这个选举进行被取消时，该部分会等待真正的反馈，因为带缓冲的，两到三个不会被消化的反馈还能处理，但如果有 10 个以上的反馈需要处理，那么就会阻塞这个部分
					// 导致阻塞  go makeRequestVote
					replyChannel <- reply
				} else {
					DPrintf5("Server %d[%s] call RequestVote of Server %d Fail ,For return fail", rf.me, rf.status(), server)
					replyChannel <- nil
				}
				wg.Done()
			}(i)
		}
	}

	total := serverNumber
	votedForMe := 1
	refused := 0
	//	netBroken := 0
	go func() {
		// Election statistics Goroutine
		flag := true // make sure only send one time
		for {
			select {
			case reply, ok := <-replyChannel:
				if !ok {
					DPrintf3("Service %d(%s) Election Statistics Goroutine End", rf.me, rf.status())
					wg2.Done()
					return
				}
				if reply != nil {
					if reply.VoteGranted {
						DPrintf3("Server %d(%s) get a success request vote reply ", rf.me, rf.status())
						votedForMe = votedForMe + 1
					} else {
						DPrintf3("Server %d(%s) get a  fail request vote reply", rf.me, rf.status())
						refused = refused + 1
					}
				} else {
					//netBroken = netBroken + 1
					DPrintf3("Server %d call the request vote of Server ? Fail", rf.me)
				}
				DPrintf3("Vote Condition: voted for %d: success %d, refused:%d, now:%f", rf.me, votedForMe, refused, float32(votedForMe)/float32(total))

				//DPrintf3("Vote Condition: voted for %d: success %d, refused:%d, netBroken:%d, now:%f", rf.me, votedForMe, refused, netBroken, float32(votedForMe)/float32(total))

				if flag && float32(votedForMe)/float32(total) > 0.5 {
					rf.becomeLeader <- true
					flag = false
					DPrintf3("Service %d(%s) can be a leader", rf.me, rf.status())
				} else if flag && float32(refused)/float32(total) > 0.5 {
					rf.becomeLeader <- false
					flag = false
					DPrintf3("Service %d(%s) can not be a leader", rf.me, rf.status())
				}
			case <-rf.cancelSelection:
				// Ask End Selection
				wg2.Done()
				DPrintf3("Service %d(%s) Election Statistics Goroutine End，For time out",rf.me,rf.status())
				return
				//case <-funcCancelSelect:
				//	// Election finish End
				//	DPrintf3("Service %d(%s) Election Statistics Goroutine End",rf.me,rf.status())
				//	return
			}
		}
	}()

	wg.Wait()
	close(replyChannel)
	wg2.Wait()
	//funcCancelSelect <- struct{}{} // close Election statistics Goroutine
	DPrintf3("Service %d(%s) end a select process", rf.me, rf.status())

}
func (rf *Raft) status() string {
	switch rf.state {
	case LEADER:
		return "LEADER"
	case CANDIDATE:
		return "CANDIDATE"
	case FOLLOWER:
		return "FOLLOWER"
	}
	return ""
}
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.state = FOLLOWER
	rf.votedFor = -1
	rf.currentTerm = 0
	rf.timeout = randInt(200, 300)
	DPrintf("Service %d's timeout is %d", rf.me, rf.timeout)
	rf.heartbeat = make(chan HeatbeatMeta)
	rf.becomeLeader = make(chan bool)
	rf.cancelSelection = make(chan struct{})
	rf.startUpCancelSelection = make(chan struct{})

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.startUp()


	return rf
}
