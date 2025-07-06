use std::rc::Rc;

use anchor_client::Client;
use anchor_client::solana_sdk::signature::Keypair;
use anchor_lang::AnchorDeserialize;
use solana_sdk::{pubkey::Pubkey, timing::timestamp};
use serde::Serialize;
use tokio::sync::mpsc;
use crate::pump_amm::{ accounts::BondingCurve, ID };

use super::grpc;

struct CandleStickData {

}

pub fn buy() {
    let p = Keypair::new();
    let client_amm = Client::new(anchor_client::Cluster::Mainnet, Rc::new(p));
    let token = "sample-token";

    let program = client_amm.program(ID).unwrap();
}

#[derive(Debug, Serialize)]
pub struct AccountInfo {
    pub time: u64,
    pub price: f64,
    pub market_cap: f64,
}

pub async fn stream_token(token: &str) -> mpsc::Receiver<AccountInfo> {
    let pump_amm_key: Pubkey = Pubkey::from_str_const("6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P");
    let token: Pubkey = Pubkey::from_str_const(token);
    let (bonding_curve, _) = Pubkey::find_program_address(
        &[b"bonding-curve", &token.to_bytes()],
        &pump_amm_key,
    );
    let auth_token = "a1fb2ed4-f5df-4688-982b-4fad1944ef0e";
    let mut raw_rx = grpc::listen_token(bonding_curve.to_string(), auth_token).await;
    let (tx, rx) = mpsc::channel(500);

    tokio::spawn(async move {
        while let Some(m) = raw_rx.recv().await {
            let mut data: &[u8] = &m.data;
            match BondingCurve::deserialize(&mut data) {
                Ok(b) => {
                    println!("GOt MCAP: {:?}", b);
                    let price: f64 = b.virtual_sol_reserves as f64/b.virtual_token_reserves as f64;
                    if tx.send(AccountInfo {
                        time: timestamp(),
                        price,
                        market_cap: price * b.token_total_supply as f64,
                    }).await.is_err() {
                        println!("pump send fail");
                        break;
                    }
                },
                _ => {}
            };
        }

    });

    return rx;
}
