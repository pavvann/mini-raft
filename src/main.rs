mod raft;
use std::{collections::HashMap};

use raft::{Message, RaftNode};
use tokio::{sync::mpsc::unbounded_channel, task};

#[tokio::main]
async fn main() {
    let node_ids = vec![1,2,3];
    let mut tx_map: HashMap<u64, tokio::sync::mpsc::UnboundedSender<Message>> = HashMap::new();
    let mut rx_map: HashMap<u64, tokio::sync::mpsc::UnboundedReceiver<Message>> = HashMap::new();

    // create channels for each node
    for &id in &node_ids {
        let (tx, rx) = unbounded_channel();
        tx_map.insert(id, tx);
        rx_map.insert(id, rx);
    }
    
    // spawn tasks for each node
    for &id in &node_ids {
        let peers: Vec<u64> = node_ids.iter().copied().filter(|x| *x != id).collect();
        let node_tx_map = tx_map.clone();
        let node_rx = rx_map.remove(&id).unwrap();

        task::spawn(async move {
            let mut node = RaftNode::new(id, peers, node_tx_map, node_rx);
            node.run().await;
        });
    }
    // sleep forever to keep main alive
    println!("🔧 Spinning up Raft cluster...");

    loop {
        tokio::time::sleep(tokio::time::Duration::from_secs(60)).await;
    }
}