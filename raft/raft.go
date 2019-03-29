package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(Command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"fmt"
	"github.com/sirupsen/logrus"
	"log"
	"sync"
	"time"
)
import "6824_2018/labrpc"

// import "bytes"
// import "labgob"

//
// as each Raft peer becomes aware that successive log Entries are
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
type entry struct {
	Command interface{}
	Term    int
}
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

	//2B
	logs []entry
	// index of highest log entry known to be committed
	// initialized to 0, increases  monotonically
	// 是 Commited 还是 applited 是已经过半提交的意思？
	commitIndex int
	// index of highest log entry applied to state machine
	// initialized to 0, increases  monotonically

	lastApplied int

	// for each server, index of the next log entry  to send to that server
	// (initialized to leader last log index + 1)
	nextIndex []int
	// for each server, index of highest log entry  known to be replicated on server
	// (initialized to 0, increases monotonically)
	matchIndex []int
	//
	syncController  syncGroutineController
	// the rwmu of commitIndex and logs


	heartbeat              chan HeatbeatMeta
	syncStart chan struct{}
	cancelSelection        chan struct{}
	startUpCancelSelection chan struct{}
	becomeLeader           chan bool
}
type syncGroutineController struct {
	mu  sync.Mutex
	syncGoroutine []bool
	cancelSyncGoroutine [] chan struct{}
}

func (s *syncGroutineController) init(serverNum int) {
	s.syncGoroutine = make([]bool,serverNum)
	s.cancelSyncGoroutine = make([]chan struct{},serverNum)
	panic("implement me")
}

func (s *syncGroutineController) isStart(server int) bool {
	s.mu.Lock()
	defer  s.mu.Unlock()
	return s.syncGoroutine[server]
}

func (s *syncGroutineController) start(server int)  {
	s.mu.Lock()
	defer  s.mu.Unlock()
	s.syncGoroutine[server] = true
}
func (s *syncGroutineController) end(server int)  {
	s.mu.Lock()
	defer  s.mu.Unlock()
	s.syncGoroutine[server] = false
	s.cancelSyncGoroutine[server] = nil
}

func (s *syncGroutineController) addCancelChannel(server int, cancel chan struct{}) {
	s.mu.Lock()
	defer  s.mu.Unlock()
	s.cancelSyncGoroutine[server] = cancel
}

func (s *syncGroutineController) removeCancelChannel(server int) {
	s.mu.Lock()
	defer  s.mu.Unlock()
	s.cancelSyncGoroutine[server] = nil
}
// 防止 start 和 addCancelChannel 被分开运行。
// 调用 cancel  之前，状态要改变为 非 leader 。
func (s *syncGroutineController) cancel() {
	s.mu.Lock()
	defer  s.mu.Unlock()
	for i:=0; i<len(s.cancelSyncGoroutine); i++ {
		if s.syncGoroutine[i] {
			// 立即取消
			s.syncGoroutine[i] = false
			if s.cancelSyncGoroutine[i] != nil {
				s.cancelSyncGoroutine[i] <- struct{}{}
				s.cancelSyncGoroutine[i] = nil

			}else{
				s.mu.Unlock()
				// unlock it ,  wait 50 ms
				// TODO Check it
				time.Sleep(50*time.Millisecond)
				s.mu.Lock()
				// do again
				if s.cancelSyncGoroutine[i] != nil {
					s.cancelSyncGoroutine[i] <- struct{}{}
					s.cancelSyncGoroutine[i] = nil

				}else{
					log.Fatalf("s.cancelSyncGoroutine[%d] aren't excpeted set",i)
				}
			}

		}

	}
}

type syncGroutineControllerIn interface {
	// init SyncGoroutine and cancelSyncGoroutine and mu
	init(serverNum int)
	//  检查该 server 的 Goroutine 是否启动
	isStart(server int) bool
	// start sync goroutine for server
	start(server int) bool
	// add cancel channel for server
	addCancelChannel(server int,cancel chan struct{})
	// remove  cancel channel for server
	removeCancelChannel(server int)
	// cancel all goroutine and rteset
	cancel()
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

	for i:=0; i < len(rf.peers) ; i ++ {
		if i != rf.me {
			rf.matchIndex[i] = -1
			rf.nextIndex[i] = len(rf.logs)
		}
	}

	rf.state = LEADER
}

func (rf *Raft) isLogSatisfy() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for _, v := range rf.matchIndex {

		if rf.commitIndex != v {
			return false
		}
	}
	return true
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
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
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
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
	// index of log entry immediately preceding new ones
	PrevLogIndex int
	// term of preLogIndex entry
	PrevLogTerm int
	// log Entries to store (empty for heartbeat )
	Entries []entry
	// leader's commitIndex
	LeaderCommit int
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
// TODO:2B
// 添加日志判断，在考虑 index 时如何返回信息
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
	// 判断发出请求的 Candidate 的日志，是否优先于当前 Server 的方式
	// If the logs have last Entries with different terms, then
	// the log with the later term is more up-to-date. If the logs
	// end with the same term, then whichever log is longer is
	// more up-to-date.
	// TODO 在这里被拒绝的请求，是否会因为 AppendEntries 的心跳包被重置成功。
	// 但是成为 Leader 的 Log 在大部分的 server 中出现。
	// 只要在实际上被大部分 server  接受的 log，会出现在 leader 之中的。
	// 如果一个 log 只被少部分同步呢？ 是否会被放弃
	currentLastLogTerm := func() int {
		if len(rf.logs) == 0 {
			return 0
		}else {
			return rf.logs[len(rf.logs)-1].Term
		}
	}()
 		if rf.currentTerm < args.Term &&
		( currentLastLogTerm < args.LastLogTerm || (len(rf.logs)-1 <= args.LastLogIndex && currentLastLogTerm == args.LastLogTerm )) {
		DPrintf3("Server %d vote Candidate %d for Term get behind", rf.me, args.CandidateId)
		reply.Term = args.Term
		reply.VoteGranted = true
		if rf.state == CANDIDATE {

			DPrintf5("Server %d(%s) try cancel startUp selection", rf.me, rf.status())
			rf.startUpCancelSelection <- struct{}{}
			DPrintf5("Server %d(%s) success  cancel startUp selection ", rf.me, rf.status())

		}
		rf.heartbeat <- HeatbeatMeta{Term: args.Term, VotedFor: args.CandidateId, TmpFlag: "RequestVote"}
		return
	} else if rf.currentTerm == args.Term && (rf.votedFor == -1 || rf.votedFor == args.CandidateId) &&
		(currentLastLogTerm < args.LastLogTerm || (len(rf.logs)-1 <= args.LastLogIndex && currentLastLogTerm == args.LastLogTerm )) {
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

// TODO 根据注入的日志信息同步日志
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	DPrintf("Server %d Accept Append Entry", rf.me)

	// 2A 没有日志功能
	// 如果为 HeatBeats 的功能
	//如果 Term < currentTerm 就返回 false （5.1 节）
	//获取这个心跳信号，意味这投票已经结束了，所有的接受该信号的人都已经变为 Leader
	// 	将这个 Raft 转换为 Leader 的 Follower，重置投票记录，如果是 Candidate 转换的话，需要取消这个 raft 端的
	rf.mu.Lock()
	defer  rf.mu.Unlock()
	if rf.currentTerm > args.Term {
		DPrintf("Server %d:Append Entry  Fail", rf.me)

		reply.Term = rf.currentTerm
		reply.Success = false
	} else {
		if len(args.Entries) == 0 {
			DPrintf("Server %d:Append Entry Deal Success, Arg:%v", rf.me, args)
			if rf.state == CANDIDATE {
				DPrintf5("Server %d(%s) try cancel startUp selection", rf.me, rf.status())
				rf.startUpCancelSelection <- struct{}{}
				DPrintf5("Server %d(%s) success  cancel startUp selection ", rf.me, rf.status())
			}
			rf.heartbeat <- HeatbeatMeta{Term: args.Term, VotedFor: -1, TmpFlag: "AppendEntries"}
			reply.Term = -1
			reply.Success = true
		}else{

			// Reply false if log doesn't contain an entry at PrevLogIndex whose term matches preLogTerm
			if len(rf.logs) < args.PrevLogIndex || rf.logs[args.PrevLogIndex].Term != args.Term {
				reply.Term = rf.currentTerm
				reply.Success = false
			}else{
				// If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it
				// Append any new Entries not already in the log
				// 直接 Log 覆盖



				// 预先删除多余的 Entries
				if args.PrevLogIndex+ len(args.Entries) < len(rf.logs) {
					rf.logs = rf.logs[:args.PrevLogIndex+len(args.Entries)]
				}

				j := 0
				for k:= args.PrevLogIndex +1; k < args.PrevLogIndex+ len(args.Entries) ; k++ {

					if k < len(rf.logs) {
						// 需要进行日志匹配
						if rf.logs[k].Term != args.Entries[j].Term {
							// 日志不匹配，删除其后，并且将新的增加到其中
							rf.logs = append(rf.logs[:k-1],args.Entries[j:]...)
							break
						}
					}else{
						// 全新的 entry， 直接依次添加即可
						rf.logs = append(rf.logs,args.Entries[j:]...)
						break
					}
					j = j + 1
				}

				rf.commitIndex = func(a int, b int) int {
					if a > b {
						return b
					}else {
						return a
					}
				}(rf.commitIndex,args.LeaderCommit)


			}
		}



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
// agreement on the next Command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// Command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the Command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	fmt.Println("rf.mu ")
	rf.mu.Lock()
	if rf.state == LEADER {

		rf.commitIndex = rf.commitIndex + 1
		rf.logs = append(rf.logs, entry{Command: command, Term: rf.currentTerm})
		term = rf.currentTerm
		index = rf.commitIndex
	} else {
		isLeader = false
	}
	rf.mu.Unlock()
	// new log start sync
	rf.syncStart <- struct{}{}

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
// 同步状态机
func (rf *Raft) sync() {
	for   {
		select {
			case <- rf.syncStart:
				// 启动的情况有两种，一种是 toleader，一种是有新的 start
				// 针对每一个 server 都启动一次 goroutine 进行同步，但每次只能启动一个，除非日志同步成功，或者获取取消信号，要不然不会被取消
				// 在 cancel 之后被启动， leader 不满足 leader 状态不会创建新的 goroutine

				for i:=0; i< len(rf.peers); i++ {
					// 没有启动过 syncGoroutine，并且日志不同，并且不是本身

					if  func() bool{
						flag := false
						rf.mu.Lock()
						if rf.state == LEADER && i != rf.me && rf.commitIndex > rf.matchIndex[i] && !rf.syncController.isStart(i) {
							flag = true
						}
						rf.mu.Unlock()
						return flag
					}() {

						rf.syncController.start(i)
						go func(i int) {
							cancel := make(chan struct{})
							unSat := make(chan struct{}) // 说明没有日志没有同步成功
							unSat <- struct{}{}
							rf.syncController.addCancelChannel(i,cancel)
							for {
								select {
								case <- cancel:
										// 防止 cancel 函数的提前移除
										rf.syncController.removeCancelChannel(i)
										return
								case <- unSat:
									    args :=  &AppendEntriesArgs{}
										rf.mu.Lock()
									    args.Term = rf.currentTerm
									    args.LeaderId = rf.me
									    args.LeaderCommit = rf.commitIndex
										// nextIndex 存放的是这一次同步日志开始的位置。
										// PrevLogIndex 在 nextIndex 的前面
									    args.PrevLogIndex = rf.nextIndex[i] - 1
									    args.Entries = make([]entry,len(rf.logs)-rf.nextIndex[i]+1)
									    copy(args.Entries,rf.logs[rf.nextIndex[i]:])
									    rf.mu.Unlock()

										reply := &AppendEntriesReply{}
										ok := rf.peers[i].Call("Raft.AppendEntries", args, reply)

										if ok {
											if reply.Success {
												rf.mu.Lock()
												// TODO 这样的一次性同步是否正确。
												rf.nextIndex[i]  = len(rf.logs)
												rf.matchIndex[i] =  len(rf.logs) - 1
												rf.mu.Unlock()
											}else{
												rf.mu.Lock()
												if reply.Term > rf.currentTerm {
													// TODO  重置 syncController,并且成为 Follower
												}else{
													rf.nextIndex[i] = rf.nextIndex[i] - 1
												}
												rf.mu.Unlock()
											}
										}else {
											logrus.Warnf("Server %d(%s) call AppendEntries(sync) of Server %d Fail ,For timeout", rf.me, rf.status(),i )
										}

										rf.mu.Lock()
										// 添加个读写锁
										if  len(rf.logs) >= rf.nextIndex[i]{
											rf.mu.Unlock()
											unSat <- struct{}{}
										}else{
											rf.mu.Unlock()
											rf.syncController.end(i)
											return
										}
								}
							}
						}(i)
					}

				}
		}
	}
}

// 伴随一次心跳，更新 commitIndex
func (rf * Raft) commitIndexUpdate() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	maxMatchIndex := -1

	// get the max value of rf.matchIndex
	for k,v := range rf.matchIndex {
		if v > maxMatchIndex && k!=rf.me {
			maxMatchIndex = v
		}
	}

	// from maxMatchIndex to rf.commitIndex , judge one by one
	for i:=maxMatchIndex; i>rf.commitIndex; i-- {
		flag := 1 // leader 一定已经 match 了
		for j:=0; j<len(rf.peers); j++ {
			if i >= rf.matchIndex[j] && j != rf.me {
				flag ++
			}
		}

		if float32(flag) / float32(len(rf.peers)) > 0.5 {
			// i 对应的 log 在 server 占据多数的同步,继续此次 matchIndex 的更新
			rf.commitIndex = i
			break
		}
	}

}

func (rf *Raft) startUp() {

	timer := time.NewTimer(time.Duration(rf.timeout) * time.Millisecond)
	DPrintf5("Server %d(%s) Startup,Timeout:%d", rf.me, rf.status(), rf.timeout)
	for {
		select {
		case <-timer.C:
			if rf.state == LEADER {
				DPrintf4("Server %d(%s) make heat beats", rf.me, rf.status())

				go rf.makeHeatBeat()

				rf.commitIndexUpdate()
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
				case <-rf.startUpCancelSelection:
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

// TODO Snyc Log according match[]
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
						// TODO 请求失败，如果 Term > currentTerm 的情况， 应该返回成 Follower
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

			args := &RequestVoteArgs{Term: rf.currentTerm, CandidateId: rf.me,LastLogIndex:len(rf.logs)-1}
			if len(rf.logs) == 0 {
				args.LastLogTerm = 0
			}else{
				args.LastLogTerm = rf.logs[len(rf.logs)-1].Term
			}
			reply := &RequestVoteReply{}
			wg.Add(1)
			go func(server int) {

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
				DPrintf3("Service %d(%s) Election Statistics Goroutine End，For time out", rf.me, rf.status())
				return
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
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
	rf.matchIndex = make([]int,len(rf.peers))
	rf.nextIndex = make([]int,len(rf.peers))
	rf.timeout = randInt(200, 300)
	rf.commitIndex = -1
	rf.lastApplied = -1
	DPrintf("Service %d's timeout is %d", rf.me, rf.timeout)

	rf.heartbeat = make(chan HeatbeatMeta)
	rf.becomeLeader = make(chan bool)
	rf.cancelSelection = make(chan struct{})
	rf.startUpCancelSelection = make(chan struct{})
	rf.syncStart  = make(chan struct{})
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.startUp()

	return rf
}
