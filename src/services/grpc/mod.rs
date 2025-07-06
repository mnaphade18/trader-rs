use std::collections::HashMap;

use futures::stream::StreamExt;
use yellowstone_grpc_client::{ClientTlsConfig, GeyserGrpcBuilder};
use yellowstone_grpc_proto::{geyser::subscribe_update::UpdateOneof, prelude::*};
use tokio::sync::mpsc;

const ENDPOINT: &str = "https://grpc.ams.shyft.to:443";
const PUMP_AMM_PROGRAM_ID: &str = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P";

pub async fn listen_token(token_bonding_curve: String, auth_token: &str) -> mpsc::Receiver<SubscribeUpdateAccountInfo>  {
    let mut connection = GeyserGrpcBuilder::from_shared(ENDPOINT).unwrap()
        .x_token(Some(auth_token)).unwrap()
        .tls_config(ClientTlsConfig::new().with_native_roots()).unwrap()
        .connect().await.unwrap();
    let mut accounts = HashMap::new();

    println!("Adding listener on token: {}", token_bonding_curve);
    accounts.insert(
        "accountData".to_owned(),
        SubscribeRequestFilterAccounts {
            account: vec![token_bonding_curve],
            owner: vec![], //vec![PUMP_AMM_PROGRAM_ID.to_string()],
            nonempty_txn_signature: None,
            filters: vec![]
        },
    );

    let request = SubscribeRequest {
            accounts,
            slots: HashMap::default(),
            transactions: HashMap::default(),
            transactions_status: HashMap::default(),
            blocks: HashMap::default(),
            blocks_meta: HashMap::default(),
            entry: HashMap::default(),
            commitment: Some(CommitmentLevel::Processed as i32),
            accounts_data_slice: Vec::default(),
            ping: None,
            from_slot: None,
        };

    let (update, mut results) = connection.subscribe_with_request(Some(request)).await.unwrap();
    println!("Subscrive complete");

    let (tx, rx) = mpsc::channel(1_000);

    results.filter_map(async move |m| {
        match m {
            Ok(inner) => {
                match inner.update_oneof {
                    Some(UpdateOneof::Account(a)) => Some(a),
                    _ => None
                }
            },
            Err(_) => None
        }
    })
    tokio::spawn(async move {
        while let Some(Ok(m)) = results.next().await {
            match m.update_oneof {
                Some(UpdateOneof::Account(a)) => {
                    println!("GOt some data");
                    match a.account {
                        Some(a) => {
                            if tx.send(a).await.is_err() {
                                println!("token message send failed");
                                break;
                            }
                        },
                        _ => {}
                    }

                }, 
                _ => {},
            }
        }
    });

    rx
}
