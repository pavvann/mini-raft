use rand::Rng;
use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tokio::time::sleep;

#[derive(Debug, Clone, PartialEq)]
enum Role {
    Follower,
    Candidate,
    Leader,
}

pub struct RaftNode {
    id: u64,
    role: Role,
    current_term: u64,
    voted_for: Option<u64>,
    election_timeout: Duration,
    last_heartbeat: Instant,

    peers: Vec<u64>,                                // ids of other nodes
    tx_map: HashMap<u64, UnboundedSender<Message>>, // senders to peers
    rx: UnboundedReceiver<Message>,                 // receiver for incoming messages

    log: Vec<(u64, String)>, //(term, entry)
    commit_index: usize,

    // leader only
    next_index: HashMap<u64, usize>,
    match_index: HashMap<u64, usize>,

    state_machine: HashMap<String, String>, // simple key value stores
    last_applied: usize,
}

impl RaftNode {
    pub fn new(
        id: u64,
        peers: Vec<u64>,
        tx_map: HashMap<u64, UnboundedSender<Message>>,
        rx: UnboundedReceiver<Message>,
    ) -> Self {
        // random election timeout between 150 and 300 ms
        let mut rng = rand::thread_rng();
        let timeout_ms = rng.gen_range(300..=500);
        Self {
            id,
            role: Role::Follower,
            current_term: 0,
            voted_for: None,
            election_timeout: Duration::from_millis(timeout_ms),
            last_heartbeat: Instant::now(),

            peers,
            tx_map,
            rx,

            log: vec![],
            commit_index: 0,
            next_index: HashMap::new(),
            match_index: HashMap::new(),

            state_machine: HashMap::new(),
            last_applied: 0,
        }
    }

    pub async fn run(&mut self) {
        loop {
            tokio::select! {
                Some(msg) = self.rx.recv() => {
                    self.handle_message(msg).await;
                } _ = sleep(Duration::from_millis(100)) => {
                    match self.role {
                        Role::Follower | Role::Candidate => {
                            let elapsed = self.last_heartbeat.elapsed();
                            if elapsed >= self.election_timeout {
                                println!("Node {} election timeout expired", self.id);
                                self.start_election().await;
                            } else {
                                sleep(self.election_timeout - elapsed).await;
                            }
                        }
                        Role::Leader => {

                            println!("Node {} sending heartbeats", self.id);
                            for peer_id in &self.peers {
                                if let Some (tx) = self.tx_map.get(peer_id) {
                                    let (resp_tx, _resp_rx) = unbounded_channel();
                                    let prev_log_index = self.next_index.get(peer_id).copied().unwrap_or(0).saturating_sub(1);
                                    let prev_log_term = self
                                        .log
                                        .get(prev_log_index)
                                        .map(|(term, _)| *term)
                                        .unwrap_or(0);
                                    let _ = tx.send(Message::AppendEntries {
                                        term: self.current_term,
                                        leader_id: self.id,
                                        prev_log_index: prev_log_index,
                                        prev_log_term: prev_log_term as u64,
                                        entries: vec![],
                                        leader_commit: self.commit_index,
                                        respond_to: resp_tx
                                    });
                                }
                            }
                            sleep(Duration::from_millis(50)).await;
                        }
                    }
                }
            }
        }
    }

    async fn handle_message(&mut self, msg: Message) {
        match msg {
            Message::RequestVote {
                term,
                candidate_id,
                respond_to,
            } => {
                if term > self.current_term {
                    self.current_term = term;
                    self.role = Role::Follower;
                    self.voted_for = None;
                }

                let vote_granted = if term == self.current_term
                    && (self.voted_for.is_none() || self.voted_for == Some(candidate_id))
                {
                    self.voted_for = Some(candidate_id);
                    true
                } else {
                    false
                };

                //  reply with vote decision
                let _ = respond_to.send(Message::RequestVoteResponse {
                    term: self.current_term,
                    vote_granted: vote_granted,
                    voted_id: self.id,
                });
            }
            Message::RequestVoteResponse {
                term,
                vote_granted,
                voted_id,
            } => {
                if term > self.current_term {
                    self.current_term = term;
                    self.role = Role::Follower;
                    self.voted_for = None;
                    return;
                }

                if self.role == Role::Candidate && vote_granted {
                    println!("Node {} received vote from {}", self.id, voted_id);
                    // TODO: track votes and become leader if majority reached
                }
            }

            Message::AppendEntries {
                term,
                leader_id: _,
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit,
                respond_to,
            } => {
                if term < self.current_term {
                    let _ = respond_to.send(Message::AppendEntriesResponse {
                        term: self.current_term,
                        success: false,
                        from_id: self.id,
                    });
                    return;
                }

                self.current_term = term;
                self.role = Role::Follower;
                self.voted_for = None;
                self.last_heartbeat = Instant::now();

                // check if log contains entry at prev log index with matching term
                if prev_log_index > 0 {
                    if self.log.len() < prev_log_index
                        || self.log[prev_log_index - 1].0 != prev_log_term as u64
                    {
                        let _ = respond_to.send(Message::AppendEntriesResponse {
                            term: self.current_term,
                            success: false,
                            from_id: self.id,
                        });
                        return;
                    }
                }

                // append any new entries not already in the log
                let mut i = 0;
                while i < entries.len() {
                    let index = prev_log_index + i;
                    if index < self.log.len() {
                        if self.log[index].1 != entries[i] {
                            self.log.truncate(index);
                            self.log.push((term, entries[i].clone()));
                        }
                    } else {
                        self.log.push((term, entries[i].clone()));
                    }
                    i += 1;
                }

                // update commit index
                if leader_commit > self.commit_index {
                    self.commit_index = std::cmp::min(leader_commit, self.log.len());
                    self.apply_committed_entries();
                    println!(
                        "Node {} updated commit index to {}",
                        self.id, self.commit_index
                    );
                }

                let _ = respond_to.send(Message::AppendEntriesResponse {
                    term: self.current_term,
                    success: true,
                    from_id: self.id,
                });
            }

            Message::AppendEntriesResponse {
                term,
                success,
                from_id,
            } => {
                if term > self.current_term {
                    self.current_term = term;
                    self.role = Role::Follower;
                    self.voted_for = None;
                    return;
                }
                if self.role == Role::Leader {
                    if success {
                        println!("Node {}: Append Entried to {} succeeded", self.id, from_id);
                        let sent_index = self.next_index.get(&from_id).copied().unwrap_or(0);
                        self.match_index.insert(from_id, sent_index);
                        self.next_index.insert(from_id, sent_index + 1);

                        self.update_commit_index();
                    } else {
                        println!("Node {}: Append Entries to {} failed", self.id, from_id);
                        self.next_index.entry(from_id).and_modify(|i| {
                            if *i > 0 {
                                *i -= 1;
                            }
                        });
                    }
                }
            }

            Message::ClientRequest { command } => {
                if self.role != Role::Leader {
                    println!("Node {} rejected client request (not leader)", self.id);
                    return;
                }

                println!("Node {} received client command: {}", self.id, command);

                // append command to own log
                self.log.push((self.current_term, command.clone()));

                // update own match_index and next_index
                let last_index = self.log.len();
                self.match_index.insert(self.id, last_index);
                self.next_index.insert(self.id, last_index + 1);

                // send appendentries with the new entry to all peers
                for &peer_id in &self.peers {
                    if let Some(tx) = self.tx_map.get(&peer_id) {
                        let prev_log_index = self
                            .next_index
                            .get(&peer_id)
                            .copied()
                            .unwrap_or(1)
                            .saturating_sub(1);

                        let prev_log_term = self
                            .log
                            .get(prev_log_index)
                            .map(|(term, _)| *term)
                            .unwrap_or(0);

                        let entries = vec![command.clone()];

                        let (resp_tx, _resp_rx) = unbounded_channel();

                        let _ = tx.send(Message::AppendEntries {
                            term: self.current_term,
                            leader_id: self.id,
                            prev_log_index,
                            prev_log_term,
                            entries,
                            leader_commit: self.commit_index,
                            respond_to: resp_tx,
                        });
                    }
                }
            }
        }
    }

    async fn start_election(&mut self) {
        println!(
            "🚀 Node {} starting election for term {}",
            self.id,
            self.current_term + 1
        );
        self.role = Role::Candidate;
        self.current_term += 1;
        self.voted_for = Some(self.id);
        self.last_heartbeat = Instant::now();

        println!(
            "NOde {} starting election for term {}",
            self.id, self.current_term
        );

        let mut votes = 1;
        let majority = (self.peers.len() + 1) / 2 + 1;

        for peer_id in &self.peers {
            if let Some(tx) = self.tx_map.get(peer_id) {
                let (resp_tx, mut resp_rx) = unbounded_channel();
                let _ = tx.send(Message::RequestVote {
                    term: self.current_term,
                    candidate_id: self.id,
                    respond_to: resp_tx,
                });

                // await single response with timeout
                if let Ok(Some(Message::RequestVoteResponse { vote_granted, .. })) =
                    tokio::time::timeout(Duration::from_millis(200), resp_rx.recv()).await
                {
                    if vote_granted {
                        votes += 1;
                        if votes >= majority {
                            self.become_leader();
                            break;
                        }
                    }
                }
            }
        }

        if self.role == Role::Candidate {
            println!("Node {} failed to get majority votes", self.id);
            // back to follower
            self.role = Role::Follower;
            self.voted_for = None;
        }
    }

    fn become_leader(&mut self) {
        self.role = Role::Leader;
        println!(
            "Node {} is now the leader for term {}",
            self.id, self.current_term
        );
        let last_log_index = self.log.len();

        self.next_index = self.peers.iter().map(|&p| (p, last_log_index)).collect();
        self.match_index = self.peers.iter().map(|&p| (p, 0)).collect();

        self.send_heartbeats();
    }

    fn send_heartbeats(&self) {
        for &peer_id in &self.peers {
            if let Some(tx) = self.tx_map.get(&peer_id) {
                let prev_log_index = self
                    .next_index
                    .get(&peer_id)
                    .copied()
                    .unwrap_or(0)
                    .saturating_sub(1);
                let prev_log_term = self.log.get(prev_log_index).map(|(t, _)| *t).unwrap_or(0);

                let entries = vec![];

                let _ = tx.send(Message::AppendEntries {
                    term: self.current_term,
                    leader_id: self.id,
                    prev_log_index,
                    prev_log_term,
                    entries,
                    leader_commit: self.commit_index,
                    respond_to: self.tx_map[&self.id].clone(),
                });
            }
        }
    }

    fn update_commit_index(&mut self) {
        let mut match_indexes: Vec<usize> = self.match_index.values().cloned().collect();
        match_indexes.push(self.log.len()); // include leader itself
        match_indexes.sort_unstable_by(|a, b| b.cmp(a)); // sort desc

        let majority_index = match_indexes[(self.peers.len()) / 2]; // majority threshold

        if majority_index > self.commit_index {
            if let Some((term, _)) = self.log.get(majority_index - 1) {
                if *term == self.current_term {
                    self.commit_index = majority_index;
                    println!(
                        "Node {} updated commit_index to {}",
                        self.id, self.commit_index
                    );
                    self.apply_committed_entries();
                }
            }
        }
    }

    fn apply_committed_entries(&mut self) {
        while self.last_applied < self.commit_index {
            self.last_applied += 1;
            let (term, entry) = &self.log[self.last_applied - 1];

            println!(
                "Node {} applying log [{}] to state machine: {:?}",
                self.id,
                self.last_applied - 1,
                entry
            );

            // for now we will assume entries are in the form "key = val"
            if let Some((key, value)) = entry.split_once('=') {
                self.state_machine
                    .insert(key.to_string(), value.to_string());
            }
        }
    }
}

#[derive(Debug)]
pub enum Message {
    RequestVote {
        term: u64,
        candidate_id: u64,
        respond_to: UnboundedSender<Message>,
    },
    RequestVoteResponse {
        term: u64,
        vote_granted: bool,
        voted_id: u64,
    },

    AppendEntries {
        term: u64,
        leader_id: u64,
        prev_log_index: usize,
        prev_log_term: u64,
        entries: Vec<String>,
        leader_commit: usize,
        respond_to: UnboundedSender<Message>,
    },
    AppendEntriesResponse {
        term: u64,
        success: bool,
        from_id: u64,
    },

    ClientRequest {
        command: String,
    },
}
