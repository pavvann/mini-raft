use rand::Rng;
use std::collections::HashMap;
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

    log: Vec<String>,
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

            log: Vec::new(),
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
                                    let _ = tx.send(Message::AppendEntries {
                                        term: self.current_term,
                                        leader_id: self.id,
                                        entries: vec![],
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
                let _ = respond_to.send(Message::RequestVoteResponse { term: self.current_term, vote_granted: vote_granted, voted_id: self.id });
            }
            Message::RequestVoteResponse { term, vote_granted, voted_id } => {
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

            Message::AppendEntries { term, leader_id: _, entries, respond_to } => {
                if term >= self.current_term {
                    self.current_term = term;
                    self.role = Role::Follower;
                    self.voted_for = None;
                    self.last_heartbeat = Instant::now();

                    self.log.extend(entries);

                    let _ = respond_to.send(Message::AppendEntriesResponse { term: self.current_term, success: true, from_id: self.id });

                } else {
                    let _ = respond_to.send(Message::AppendEntriesResponse { term: self.current_term, success: false, from_id: self.id });
                }
            }

            Message::AppendEntriesResponse { term, success, from_id } => {
                if term > self.current_term {
                    self.current_term = term;
                    self.role = Role::Follower;
                    self.voted_for = None;
                    return;
                }
                if self.role == Role::Leader {
                    if success {
                        println!("Node {}: Append Entried to {} succeeded", self.id, from_id);
                    }
                    else {
                        println!("Node {}: Append Entries to {} failed", self.id, from_id);
                    }
                }
            }
        }
    }

    async fn start_election(&mut self) {
        println!("🚀 Node {} starting election for term {}", self.id, self.current_term + 1);
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
                let _ = tx.send(Message::RequestVote { term: self.current_term, candidate_id: self.id, respond_to: resp_tx });

                // await single response with timeout
                if let Ok(Some(Message::RequestVoteResponse {vote_granted, ..})) = 
                    tokio::time::timeout(Duration::from_millis(200), resp_rx.recv()).await {
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
        entries: Vec<String>,
        respond_to: UnboundedSender<Message>,
    },
    AppendEntriesResponse {
        term: u64,
        success: bool,
        from_id: u64,
    },
}
