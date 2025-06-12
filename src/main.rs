mod raft;
use std::{collections::HashMap, time::Duration};

use raft::{Message, RaftNode};
use tokio::{sync::mpsc::unbounded_channel, task};
use tokio::time::sleep;


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

    let tx_map_clone = tx_map.clone();
    tokio::spawn(async move {
      sleep(Duration::from_secs(5)).await;
      let leader_id = 3;
      if let Some(tx) = tx_map_clone.get(&leader_id) {
        let _ = tx.send(Message::ClientRequest { command: "x = 42".to_string() });
      }  
    });
    loop {
        tokio::time::sleep(tokio::time::Duration::from_secs(60)).await;
    }
}