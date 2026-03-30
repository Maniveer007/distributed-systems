```python
import threading
import time
import random
import queue
from typing import Dict, List, Any, Optional, Tuple, Set

"""
Raft Consensus Algorithm Implementation.

This module implements a simplified version of the Raft consensus algorithm,
covering leader election and log replication using only Python's standard library.

Concept Overview:
Raft is a consensus algorithm designed to be understandable and provide a strong
consistency guarantee in a distributed system. It achieves this by electing
a single leader that is responsible for managing the replicated log. All changes
to the system state go through the leader.

The core components of Raft are:
1.  Leader Election: Servers transition between Follower, Candidate, and Leader states.
    Followers listen for heartbeats or log entries from a leader. If a follower
    doesn't hear from a leader for an "election timeout" period, it becomes a
    candidate, increments its term, votes for itself, and requests votes from peers.
    If it receives votes from a majority, it becomes the leader.
2.  Log Replication: The leader accepts client commands, appends them to its log,
    and then replicates them to followers. Once an entry is safely replicated
    to a majority of servers, it is considered "committed" and can be applied
    to the state machine.
3.  Safety: Raft guarantees "state machine safety" (all committed entries will
    eventually be applied in the same order on all servers) and "leader
    completeness" (leaders always have all committed log entries).

Use Cases:
-   Distributed databases (e.g., etcd, ZooKeeper's successor).
-   Consistent distributed key-value stores.
-   Replicated state machines.
-   Building highly available and fault-tolerant services.

Guarantees:
-   **Consistency:** All non-faulty servers agree on the sequence of commands
    applied to the state machine.
-   **Availability:** The system can continue to operate and make progress as long
    as a majority of servers are healthy and can communicate.
-   **Fault Tolerance:** Tolerates up to (N-1)/2 server failures, where N is the
    total number of servers.

Trade-offs:
-   **Complexity:** While simpler than Paxos, implementing Raft correctly can still
    be complex due to various edge cases and timing considerations.
-   **Performance:** All writes must go through the leader and be replicated,
    which can introduce latency compared to eventually consistent systems.
-   **Network Dependence:** Requires a reliable network for robust operation.
    Network partitions can temporarily hinder progress until resolved.
"""

class LogEntry:
    """Represents an entry in the Raft log."""
    def __init__(self, term: int, command: Any):
        self.term = term
        self.command = command

    def __repr__(self) -> str:
        return f"LogEntry(term={self.term}, cmd='{self.command}')"

class RaftServer:
    FOLLOWER = "follower"
    CANDIDATE = "candidate"
    LEADER = "leader"

    def __init__(self, id: int, peers: List[int], message_queues: Dict[int, queue.Queue]):
        self.id = id
        self.peers = peers # List of peer server IDs
        self.message_queues = message_queues # Shared message queues for communication

        # Persistent state on all servers (must be updated to stable storage in real Raft)
        self.current_term = 0
        self.voted_for: Optional[int] = None # Candidate ID that received vote in current term
        self.log: List[LogEntry] = [] # Log entries, 0-indexed (Raft log is 1-indexed)

        # Volatile state on all servers
        self.commit_index = 0 # Index of highest log entry known to be committed (1-indexed)
        self.last_applied = 0 # Index of highest log entry applied to state machine (1-indexed)

        # Volatile state on leaders (reinitialized after election)
        self.next_index: Dict[int, int] = {} # For each server, index of the next log entry to send to that server (1-indexed)
        self.match_index: Dict[int, int] = {} # For each server, index of highest log entry known to be replicated on server (1-indexed)

        self.state = self.FOLLOWER
        self.leader_id: Optional[int] = None

        # Timers and concurrency
        self.election_timeout_ms = random.randrange(150, 300) # Randomized election timeout
        self.heartbeat_interval_ms = 50 # Heartbeat interval for leaders
        self.election_timer: Optional[threading.Timer] = None
        self.heartbeat_timer: Optional[threading.Timer] = None
        self._lock = threading.Lock() # To protect shared state
        self._thread: Optional[threading.Thread] = None
        self._running = False
        self.inbox = self.message_queues[self.id] # This server's incoming message queue

        # Election-specific volatile state (cleared after election)
        self._votes_received: int = 0
        self._voters: Set[int] = set()

        print(f"Server {self.id} initialized as {self.state}.")

    def _reset_election_timer(self) -> None:
        """Resets the election timer, canceling any existing one."""
        if self.election_timer:
            self.election_timer.cancel()
        self.election_timeout_ms = random.randrange(150, 300) # Re-randomize for next election
        self.election_timer = threading.Timer(self.election_timeout_ms / 1000.0, self._election_timeout)
        self.election_timer.start()

    def _election_timeout(self) -> None:
        """Called when the election timer expires. Initiates a new election."""
        with self._lock:
            if not self._running: # Do not start election if server is stopping
                return
            print(f"Server {self.id} election timeout. Becoming CANDIDATE (Term {self.current_term + 1}).")
            self.state = self.CANDIDATE
            self.leader_id = None # Clear leader ID as we are starting an election
            self._start_election()

    def _start_election(self) -> None:
        """Starts a new election by incrementing term, voting for self, and sending RequestVote RPCs."""
        self.current_term += 1 # Increment current term
        self.voted_for = self.id # Vote for self
        self._votes_received = 1 # Initialize votes count (for self)
        self._voters = {self.id} # Track voters

        last_log_index = len(self.log) # 1-indexed last log entry
        last_log_term = self.log[-1].term if self.log else 0

        print(f"Server {self.id} (Term {self.current_term}) starting election. Log: len {len(self.log)}, last term {last_log_term}")
        self._reset_election_timer() # Reset election timer for this new election

        # Send RequestVote RPCs to all other servers
        for peer_id in self.peers:
            self._send_message(peer_id, "RequestVote", {
                "term": self.current_term,
                "candidate_id": self.id,
                "last_log_index": last_log_index,
                "last_log_term": last_log_term
            })

    def _become_leader(self) -> None:
        """Transitions server to Leader state."""
        with self._lock:
            self.state = self.LEADER
            self.leader_id = self.id
            print(f"Server {self.id} (Term {self.current_term}) BECOMES LEADER!")

            # Initialize nextIndex and matchIndex for all followers
            for peer_id in self.peers:
                self.next_index[peer_id] = len(self.log) + 1 # Next entry to send (1-indexed)
                self.match_index[peer_id] = 0 # Highest replicated log entry (1-indexed)

            # Cancel election timer and start heartbeat timer
            if self.election_timer:
                self.election_timer.cancel()
            self._start_heartbeats() # Immediately send first heartbeats

            # Clear election-specific volatile state
            self._votes_received = 0
            self._voters = set()

    def _start_heartbeats(self) -> None:
        """Starts the periodic heartbeat mechanism for the leader."""
        if self.heartbeat_timer:
            self.heartbeat_timer.cancel()
        
        # Send heartbeats (empty AppendEntries RPCs)
        self._send_heartbeats()
        # Schedule next heartbeat
        self.heartbeat_timer = threading.Timer(self.heartbeat_interval_ms / 1000.0, self._start_heartbeats)
        self.heartbeat_timer.start()

    def _send_heartbeats(self) -> None:
        """Sends empty AppendEntries RPCs to all followers as heartbeats."""
        if self.state != self.LEADER or not self._running:
            return

        with self._lock:
            for peer_id in self.peers:
                # Determine prevLogIndex and prevLogTerm for the heartbeat
                prev_log_index = self.next_index.get(peer_id, 1) - 1 # 1-indexed
                prev_log_term = 0
                if prev_log_index > 0: # If there's a preceding entry
                    prev_log_term = self.log[prev_log_index - 1].term # 0-indexed access

                self._send_message(peer_id, "AppendEntries", {
                    "term": self.current_term,
                    "leader_id": self.id,
                    "prev_log_index": prev_log_index,
                    "prev_log_term": prev_log_term,
                    "entries": [], # Heartbeat has no new entries
                    "leader_commit": self.commit_index
                })

    def _send_message(self, target_id: int, msg_type: str, payload: Dict[str, Any]) -> None:
        """Simulates sending a message to a peer by putting it in their message queue."""
        message = {"sender_id": self.id, "type": msg_type, "payload": payload}
        try:
            self.message_queues[target_id].put(message, timeout=0.1)
        except queue.Full:
            print(f"Warning: Message queue for {target_id} is full. Dropping message from {self.id}.")

    def _handle_request_vote(self, message: Dict[str, Any]) -> None:
        """Handles an incoming RequestVote RPC."""
        sender_id = message["sender_id"]
        payload = message["payload"]
        term = payload["term"]
        candidate_id = payload["candidate_id"]
        last_log_index = payload["last_log_index"]
        last_log_term = payload["last_log_term"]

        with self._lock:
            vote_granted = False
            
            # Raft Rule: If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower
            if term > self.current_term:
                self._step_down(term)

            # Raft Rule: Reply false if term < currentTerm
            if term < self.current_term:
                vote_granted = False
            # Raft Rule: If votedFor is null or candidateId, and candidate's log is at least as up-to-date as receiver's log, grant vote
            else: # term >= self.current_term
                log_up_to_date = self._is_log_up_to_date(last_log_index, last_log_term)
                if (self.voted_for is None or self.voted_for == candidate_id) and log_up_to_date:
                    self.voted_for = candidate_id
                    vote_granted = True
                    self._reset_election_timer() # Granting vote means we've heard from a valid candidate
                    # print(f"Server {self.id} (Term {self.current_term}) GRANTED vote for {candidate_id}.")
                else:
                    vote_granted = False

            self._send_message(sender_id, "RequestVoteResponse", {
                "term": self.current_term,
                "vote_granted": vote_granted
            })

    def _handle_request_vote_response(self, message: Dict[str, Any]) -> None:
        """Handles an incoming RequestVoteResponse RPC."""
        sender_id = message["sender_id"]
        payload = message["payload"]
        term = payload["term"]
        vote_granted = payload["vote_granted"]

        with self._lock:
            # Raft Rule: If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower
            if term > self.current_term:
                self._step_down(term)
                return

            if self.state != self.CANDIDATE or term < self.current_term: # Ignore if not candidate or for an old term
                return

            if vote_granted and sender_id not in self._voters:
                self._votes_received += 1
                self._voters.add(sender_id)
                
                # Check for majority dynamically: N total servers, majority is floor(N/2) + 1
                total_servers = len(self.peers) + 1
                if self._votes_received > total_servers // 2:
                    if self.state == self.CANDIDATE: # Double check state to avoid multiple leaders
                        self._become_leader()
            # else: Vote denied, or already voted, or duplicate response, or not for current term.

    def _handle_append_entries(self, message: Dict[str, Any]) -> None:
        """Handles an incoming AppendEntries RPC (heartbeat or log replication)."""
        sender_id = message["sender_id"]
        payload = message["payload"]
        term = payload["term"]
        leader_id = payload["leader_id"]
        prev_log_index = payload["prev_log_index"]
        prev_log_term = payload["prev_log_term"]
        entries: List[LogEntry] = payload["entries"]
        leader_commit = payload["leader_commit"]

        with self._lock:
            reply_success = False

            # Raft Rule: If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower
            if term > self.current_term:
                self._step_down(term)

            # Raft Rule: Reply false if term < currentTerm
            if term < self.current_term:
                reply_success = False
            else: # term >= self.current_term
                self.state = self.FOLLOWER # Convert to follower if not already
                self.leader_id = leader_id
                self._reset_election_timer() # Reset election timer (heard from leader)
                self.voted_for = None # Clear voted_for as new term/leader is established

                # Raft Rule: Reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm
                # prev_log_index is 1-indexed. log is 0-indexed.
                if prev_log_index > 0 and (prev_log_index > len(self.log) or self.log[prev_log_index - 1].term != prev_log_term):
                    reply_success = False
                else:
                    reply_success = True
                    # If an existing entry conflicts with a new one, delete existing and all that follow
                    # Then append any new entries not already in the log
                    new_entries_start_idx = prev_log_index # 1-indexed point where new entries should start
                    for i, entry in enumerate(entries):
                        log_idx = new_entries_start_idx + i # 1-indexed target position
                        if log_idx <= len(self.log): # If there's an existing entry at this position
                            if self.log[log_idx - 1].term != entry.term: # Conflict
                                self.log = self.log[:log_idx - 1] # Truncate from this point
                                self.log.append(entry)
                        else: # Past end of current log, just append
                            self.log.append(entry)

                    # Raft Rule: If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
                    if leader_commit > self.commit_index:
                        last_new_entry_index = prev_log_index + len(entries) # Highest 1-indexed index received
                        self.commit_index = min(leader_commit, last_new_entry_index)

                    self._apply_log_entries() # Apply committed entries to state machine

            # Send AppendEntriesResponse
            self._send_message(sender_id, "AppendEntriesResponse", {
                "term": self.current_term,
                "success": reply_success,
                "match_index": prev_log_index + len(entries) if reply_success else 0 # Highest index successfully replicated
            })

    def _handle_append_entries_response(self, message: Dict[str, Any]) -> None:
        """Handles an incoming AppendEntriesResponse RPC."""
        sender_id = message["sender_id"]
        payload = message["payload"]
        term = payload["term"]
        success = payload["success"]
        match_index = payload["match_index"] # Highest 1-indexed index successfully replicated

        with self._lock:
            # Raft Rule: If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower
            if term > self.current_term:
                self._step_down(term)
                return

            if self.state != self.LEADER or term < self.current_term: # Ignore if not leader or old term
                return

            if success:
                # Raft Rule: If successful: update nextIndex and matchIndex for follower
                self.match_index[sender_id] = max(self.match_index.get(sender_id, 0), match_index)
                self.next_index[sender_id] = self.match_index[sender_id] + 1

                # Raft Rule: If there exists an N such that N > commitIndex, a majority of matchIndex[i] >= N,
                # and log[N].term == currentTerm: set commitIndex = N.
                # Find the largest N where N > commitIndex, and a majority of peers have replicated log[N]
                for N in range(len(self.log), self.commit_index, -1): # Iterate N from current max log length down to commit_index + 1
                    if self.log[N - 1].term != self.current_term: # Cannot commit entries from previous terms this way
                        break
                    
                    count = 1 # Leader itself has replicated the entry
                    for peer_id in self.peers:
                        if self.match_index.get(peer_id, 0) >= N:
                            count += 1
                    
                    total_servers = len(self.peers) + 1
                    if count > total_servers // 2: # Check for majority
                        self.commit_index = N
                        print(f"Leader {self.id} committed index {self.commit_index}.")
                        self._apply_log_entries()
                        break
            else:
                # Raft Rule: If AppendEntries fails because of log inconsistency:
                # decrement nextIndex and retry.
                self.next_index[sender_id] = max(1, self.next_index.get(sender_id, 1) - 1)

    def _is_log_up_to_date(self, candidate_last_log_index: int, candidate_last_log_term: int) -> bool:
        """Checks if a candidate's log is at least as up-to-date as the receiver's."""
        last_log_index = len(self.log) # 1-indexed
        last_log_term = self.log[-1].term if self.log else 0

        # Raft Rule: A candidate's log is at least as up-to-date if:
        # 1. Its last term is greater than the receiver's last term.
        # 2. Or, if terms are the same, its log is at least as long.
        if candidate_last_log_term > last_log_term:
            return True
        elif candidate_last_log_term == last_log_term:
            return candidate_last_log_index >= last_log_index
        else:
            return False

    def _step_down(self, new_term: int) -> None:
        """Converts the server to a follower, updating its term."""
        if self.current_term < new_term:
            self.current_term = new_term
            self.voted_for = None # Reset vote for new term
        self.state = self.FOLLOWER
        self.leader_id = None
        if self.heartbeat_timer: # Cancel leader's heartbeat if it was leader
            self.heartbeat_timer.cancel()
            self.heartbeat_timer = None
        self._reset_election_timer() # Start listening for heartbeats/elections again
        print(f"Server {self.id} stepped down to FOLLOWER (Term {self.current_term}).")

    def _apply_log_entries(self) -> None:
        """Applies committed log entries to the state machine."""
        while self.last_applied < self.commit_index:
            self.last_applied += 1
            entry = self.log[self.last_applied - 1] # 0-indexed access for 1-indexed last_applied
            # In a real system, this would involve executing the command
            # against the application's state machine. For demo, we just print.
            print(f"Server {self.id} applying command: '{entry.command}' (Log Index {self.last_applied}, Term {entry.term})")

    def _process_inbox(self) -> None:
        """Processes messages from the inbox."""
        while not self.inbox.empty():
            try:
                message = self.inbox.get_nowait()
                msg_type = message["type"]

                # Dispatch message handling based on type
                if msg_type == "RequestVote":
                    self._handle_request_vote(message)
                elif msg_type == "RequestVoteResponse":
                    self._handle_request_vote_response(message)
                elif msg_type == "AppendEntries":
                    self._handle_append_entries(message)
                elif msg_type == "AppendEntriesResponse":
                    self._handle_append_entries_response(message)
                elif msg_type == "ClientCommand":
                    self._handle_client_command(message)

            except queue.Empty: # Should not happen with get_nowait
                break 
            except Exception as e:
                print(f"Server {self.id} error processing message: {e}")

    def _handle_client_command(self, message: Dict[str, Any]) -> None:
        """Handles a client command received by the server."""
        command = message["payload"]["command"]
        with self._lock:
            if self.state == self.LEADER:
                # Raft Rule: If command received from client, append entry to local log,
                # respond after entry has been applied to state machine
                new_entry = LogEntry(self.current_term, command)
                self.log.append(new_entry)
                print(f"Leader {self.id} received client command '{command}'. Appended to log at index {len(self.log)}.")

                # Immediately attempt to replicate to followers
                self._replicate_log()
            else:
                # Raft Rule: If client contacts a non-leader, the non-leader redirects
                # the client to the current leader, or if it doesn't know, it denies.
                # In a real system, would send back leader_id or error.
                pass # Silently drop for this demo

    def _replicate_log(self) -> None:
        """Sends new log entries to followers (called by leader)."""
        if self.state != self.LEADER or not self._running:
            return

        with self._lock:
            for peer_id in self.peers:
                next_idx = self.next_index.get(peer_id, 1) # Next 1-indexed log entry to send
                
                if len(self.log) >= next_idx: # If there are entries to send
                    entries_to_send = self.log[next_idx - 1:] # Slice (0-indexed) from next_idx
                    
                    prev_log_index = next_idx - 1 # 1-indexed index of entry before entries_to_send
                    prev_log_term = 0
                    if prev_log_index > 0:
                        prev_log_term = self.log[prev_log_index - 1].term # 0-indexed access

                    self._send_message(peer_id, "AppendEntries", {
                        "term": self.current_term,
                        "leader_id": self.id,
                        "prev_log_index": prev_log_index,
                        "prev_log_term": prev_log_term,
                        "entries": entries_to_send,
                        "leader_commit": self.commit_index
                    })

    def _run(self) -> None:
        """Main loop for the Raft server thread."""
        self._running = True
        self._reset_election_timer() # Start election timer initially for all followers

        while self._running:
            self._process_inbox()

            with self._lock:
                # Leaders need to continuously check if followers need log entries
                # and send them, even if no new client commands.
                if self.state == self.LEADER:
                    self._replicate_log()
            
            time.sleep(0.01) # Small sleep to prevent busy-waiting

    def start(self) -> None:
        """Starts the server's main loop in a new thread."""
        self._thread = threading.Thread(target=self._run)
        self._thread.daemon = True # Allow main program to exit even if threads are running
        self._thread.start()

    def stop(self) -> None:
        """Stops the server thread."""
        print(f"Server {self.id} stopping.")
        self._running = False
        if self.election_timer:
            self.election_timer.cancel()
        if self.heartbeat_timer:
            self.heartbeat_timer.cancel()
        if self._thread:
            self._thread.join(timeout=1.0) # Wait for thread to finish
            if self._thread.is_alive():
                print(f"Warning: Server {self.id} thread did not terminate gracefully.")

    def client_request(self, command: Any) -> None:
        """Simulates a client sending a command to this server."""
        self._send_message(self.id, "ClientCommand", {"command": command})


# --- Testing and Demo ---

def test_single_leader_election() -> None:
    """
    Tests if a single leader is elected in a cluster of 3 servers.
    """
    print("\n--- Running Test: Single Leader Election (3 Servers) ---")
    num_servers = 3
    message_queues: Dict[int, queue.Queue] = {i: queue.Queue() for i in range(num_servers)}
    servers: List[RaftServer] = []
    
    for i in range(num_servers):
        peers = [p for p in range(num_servers) if p != i]
        server = RaftServer(i, peers, message_queues)
        servers.append(server)

    for server in servers:
        server.start()

    leader_found = False
    leader_id: Optional[int] = None
    timeout = time.monotonic() + 5 # 5 seconds timeout for election
    while time.monotonic() < timeout and not leader_found:
        for server in servers:
            with server._lock:
                if server.state == RaftServer.LEADER:
                    leader_found = True
                    leader_id = server.id
                    break
        time.sleep(0.1)

    assert leader_found, "Leader was not elected within the timeout period."
    print(f"SUCCESS: Leader {leader_id} elected in Term {servers[leader_id].current_term}.")

    leader_count = 0
    for server in servers:
        with server._lock:
            if server.state == RaftServer.LEADER:
                leader_count += 1
    assert leader_count == 1, f"Expected 1 leader, but found {leader_count}."
    print("SUCCESS: Only one leader exists.")

    for server in servers:
        server.stop()
    print("--- Test: Single Leader Election COMPLETE ---\n")

def test_log_replication() -> None:
    """
    Tests if a command issued to the leader is replicated and committed by followers.
    """
    print("\n--- Running Test: Log Replication (3 Servers) ---")
    num_servers = 3
    message_queues: Dict[int, queue.Queue] = {i: queue.Queue() for i in range(num_servers)}
    servers: List[RaftServer] = []
    
    for i in range(num_servers):
        peers = [p for p in range(num_servers) if p != i]
        server = RaftServer(i, peers, message_queues)
        servers.append(server)

    for server in servers:
        server.start()

    leader_id: Optional[int] = None
    timeout = time.monotonic() + 5
    while time.monotonic() < timeout and leader_id is None:
        for server in servers:
            with server._lock:
                if server.state == RaftServer.LEADER:
                    leader_id = server.id
                    break
        time.sleep(0.1)
    assert leader_id is not None, "No leader found for log replication test."
    print(f"Leader {leader_id} identified for replication test.")

    command_to_replicate = "SET x = 1"
    leader_server = servers[leader_id]
    leader_server.client_request(command_to_replicate)
    print(f"Client sent command '{command_to_replicate}' to leader {leader_id}.")

    all_committed = False
    timeout = time.monotonic() + 5 # Give it time to replicate
    while time.monotonic() < timeout and not all_committed:
        all_committed = True
        for server in servers:
            with server._lock:
                # Check if the command exists in log and is committed/applied (last_applied >= 1)
                if not any(entry.command == command_to_replicate for entry in server.log) or server.last_applied < 1:
                    all_committed = False
                    break
        time.sleep(0.05)
    
    assert all_committed, "Command was not replicated and committed by all servers."
    print("SUCCESS: Command replicated and committed by all servers.")
    for server in servers:
        with server._lock:
            assert any(entry.command == command_to_replicate for entry in server.log), \
                f"Server {server.id} did not have the command in its log."
            assert server.last_applied >= 1, f"Server {server.id} did not apply the command."
            print(f"  Server {server.id} state: log len={len(server.log)}, commit_idx={server.commit_index}, last_applied={server.last_applied}")

    for server in servers:
        server.stop()
    print("--- Test: Log Replication COMPLETE ---\n")


if __name__ == "__main__":
    print("--- Raft Consensus Algorithm Demo ---")

    test_single_leader_election()
    test_log_replication()

    print("\n--- Starting Full Raft Cluster Demo (5 Servers) ---")
    num_demo_servers = 5
    demo_message_queues: Dict[int, queue.Queue] = {i: queue.Queue() for i in range(num_demo_servers)}
    demo_servers: List[RaftServer] = []

    for i in range(num_demo_servers):
        peers = [p for p in range(num_demo_servers) if p != i]
        server = RaftServer(i, peers, demo_message_queues)
        demo_servers.append(server)

    for server in demo_servers:
        server.start()

    print("\nWaiting for leader election (up to 10 seconds)...")
    leader: Optional[RaftServer] = None
    start_time = time.monotonic()
    while time.monotonic() - start_time < 10:
        for s in demo_servers:
            with s._lock:
                if s.state == RaftServer.LEADER:
                    leader = s
                    break
        if leader:
            break
        time.sleep(0.1)

    if leader:
        print(f"\nLeader {leader.id} elected in Term {leader.current_term}.")
        
        print("\nSending commands to the leader...")
        leader.client_request("command_A")
        time.sleep(0.5)
        leader.client_request("command_B")
        time.sleep(0.5)
        leader.client_request("command_C")
        time.sleep(1.0) # Give time for replication

        print(f"\nSimulating Leader {leader.id} crash...")
        leader_id_before_crash = leader.id
        leader.stop() # "Crash" the leader
        demo_servers.remove(leader) # Remove from list for re-election

        print("\nWaiting for new leader election (up to 10 seconds)...")
        new_leader: Optional[RaftServer] = None
        start_time = time.monotonic()
        while time.monotonic() - start_time < 10:
            for s in demo_servers:
                with s._lock:
                    if s.state == RaftServer.LEADER:
                        new_leader = s
                        break
            if new_leader:
                break
            time.sleep(0.1)
        
        if new_leader:
            print(f"\nNew Leader {new_leader.id} elected in Term {new_leader.current_term}.")
            new_leader.client_request("command_D_after_crash")
            time.sleep(1.0) # Give time for replication
        else:
            print("\nFailed to elect a new leader after crash.")

        print("\nFinal state of all active servers:")
        for s in demo_servers:
            with s._lock:
                print(f"Server {s.id} (State: {s.state}, Term: {s.current_term}, Log Length: {len(s.log)}, Commit Index: {s.commit_index}, Last Applied: {s.last_applied})")
                print(f"  Log entries: {[e.command for e in s.log[:s.last_applied]]}") # Only print applied ones
        
    else:
        print("\nNo leader was elected in the demo simulation.")

    print("\nStopping all remaining demo servers...")
    for server in demo_servers:
        server.stop()
    print("--- Raft Consensus Algorithm Demo COMPLETE ---")
```