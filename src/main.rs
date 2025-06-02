mod raft;
use raft::RaftNode;

#[tokio::main]
async fn main () {
    let mut node = RaftNode::new(1);
    node.run().await;
}