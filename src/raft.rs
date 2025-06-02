use rand::Rng;
use std::time::{Duration, Instant};
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
}

impl RaftNode {
    pub fn new(id: u64) -> Self {
        // random election timeout between 150 and 300 ms
        let mut rng = rand::thread_rng();
        let timeout_ms = rng.gen_range(150..=300);

        Self {
            id,
            role: Role::Follower,
            current_term: 0,
            voted_for: None,
            election_timeout: Duration::from_millis(timeout_ms),
            last_heartbeat: Instant::now()
        }
    }

    pub async fn run(&mut self) {
        loop {
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
                    // for now just wait before sending next heartbeat
                    sleep (Duration::from_millis(50)).await;
                }
            }
        }
    } 
    async fn start_election(&mut self) {
        self.role = Role::Candidate;
        self.current_term += 1;
        self.voted_for = Some(self.id);
        self.last_heartbeat = Instant::now();

        println!("NOde {} starting election for term {}", self.id, self.current_term);

        // TODO: Request votes from peers here
        // for now become leader immediately to keep it simple
        self.become_leader();
    }

    fn become_leader(&mut self) {
        self.role = Role::Leader;
        println!("Node {} is now the leader for term {}", self.id, self.current_term);
    }
}