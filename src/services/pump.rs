use std::{collections::HashMap, rc::Rc};

use anchor_client::Client;
use anchor_client::solana_sdk::signature::Keypair;
use anchor_lang::AnchorDeserialize;
use futures::{future, Stream, stream::StreamExt};
use solana_sdk::{pubkey::Pubkey, timing::timestamp};
use serde::Serialize;
use crate::pump_amm::{ accounts::BondingCurve, ID };

use super::grpc;

#[derive(Debug, Clone, Copy, Serialize)]
pub struct CandleStickData {
    pub timestamp: u64,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
}

pub fn buy() {
    let p = Keypair::new();
    let client_amm = Client::new(anchor_client::Cluster::Mainnet, Rc::new(p));
    let _token = "sample-token";

    let _program = client_amm.program(ID).unwrap();
}

#[derive(Debug, Serialize)]
pub struct AccountInfo {
    pub time: u64,
    pub price: f64,
    pub market_cap: f64,
}

pub async fn stream_token(token: &str) -> impl Stream<Item = CandleStickData> {
    let pump_amm_key: Pubkey = Pubkey::from_str_const("6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P");
    let token: Pubkey = Pubkey::from_str_const(token);
    let (bonding_curve, _) = Pubkey::find_program_address(
        &[b"bonding-curve", &token.to_bytes()],
        &pump_amm_key,
    );
    let auth_token = "a1fb2ed4-f5df-4688-982b-4fad1944ef0e";
    let raw_rx = grpc::listen_token(bonding_curve.to_string(), auth_token).await;

    let mut time_map: HashMap<u64, CandleStickData> = HashMap::new();

    raw_rx.filter_map(move |m| {
        let mut data: &[u8] = &m.data;
        match BondingCurve::deserialize(&mut data) {
            Ok(b) => {
                let value: f64 = b.virtual_sol_reserves as f64/b.virtual_token_reserves as f64;
                let market_cap = value * b.token_total_supply as f64;
                let price = market_cap;

                let curr_time = timestamp()/1000;
                if let Some(c) = time_map.get_mut(&curr_time) {
                    if price > c.high {
                        c.high = price;
                    }
                    if price < c.low {
                        c.low = price;
                    }
                    c.close = price;

                    future::ready(Some(c.clone()))
                } else {
                    let c = CandleStickData {
                        timestamp: curr_time,
                        open: price,
                        high: price,
                        close: price,
                        low: price,
                    };
                    time_map.insert(curr_time, c.clone());

                    future::ready(Some(c))
                }
            },
            _ => future::ready(None)
        }
    })
}
