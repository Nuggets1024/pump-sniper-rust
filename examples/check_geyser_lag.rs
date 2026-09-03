use futures::{SinkExt, StreamExt};
use pump_sniper::constants::{PUMP_AMM_PROGRAM, PUMP_MINT_AUTHORITY, PUMP_PROGRAM};
use pump_sniper::pump::{contains_create_instruction, decode_transactions};
use solana_client::rpc_client::RpcClient;
use solana_commitment_config::CommitmentConfig;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::watch;
use yellowstone_grpc_client::GeyserGrpcClient;
use yellowstone_grpc_proto::prelude::{
    subscribe_update::UpdateOneof, CommitmentLevel, SubscribeRequest, SubscribeRequestFilterSlots,
    SubscribeRequestFilterTransactions,
};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let geyser_url = std::env::var("GEYSER_URL")?;
    let rpc_url = std::env::var("RPC_URL")?;
    let seconds = std::env::var("SAMPLE_SECONDS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(30);
    let include_transactions = std::env::var("INCLUDE_TRANSACTIONS")
        .map(|value| value != "false")
        .unwrap_or(true);
    let include_amm = std::env::var("INCLUDE_AMM")
        .map(|value| value != "false")
        .unwrap_or(true);
    let create_only = std::env::var("CREATE_ONLY")
        .map(|value| value == "true")
        .unwrap_or(false);
    let switch_to_mint = std::env::var("SWITCH_TO_MINT")
        .map(|value| value == "true")
        .unwrap_or(false);

    let rpc = Arc::new(RpcClient::new_with_commitment(
        rpc_url,
        CommitmentConfig::processed(),
    ));
    let rpc_tip = spawn_rpc_sampler(rpc);
    let mut client = GeyserGrpcClient::build_from_shared(geyser_url)?
        .connect()
        .await?;
    let (mut sink, mut stream) = client.subscribe().await?;

    let mut slots = HashMap::new();
    slots.insert(
        "tip".to_owned(),
        SubscribeRequestFilterSlots {
            filter_by_commitment: Some(true),
            interslot_updates: Some(false),
        },
    );
    let mut transactions = HashMap::new();
    if include_transactions {
        let mut accounts = vec![PUMP_PROGRAM.to_owned()];
        if include_amm {
            accounts.push(PUMP_AMM_PROGRAM.to_owned());
        }
        transactions.insert(
            "pump".to_owned(),
            SubscribeRequestFilterTransactions {
                vote: Some(false),
                failed: Some(false),
                account_include: accounts,
                account_required: create_only
                    .then(|| vec![PUMP_MINT_AUTHORITY.to_owned()])
                    .unwrap_or_default(),
                ..Default::default()
            },
        );
    }
    sink.send(SubscribeRequest {
        slots,
        transactions,
        commitment: Some(CommitmentLevel::Processed as i32),
        ..Default::default()
    })
    .await?;

    let started = Instant::now();
    let mut tick = tokio::time::interval(Duration::from_secs(1));
    let mut geyser_slot = None::<(u64, Instant)>;
    let mut pump_slot = None::<(u64, Instant)>;
    let mut transaction_count = 0u64;
    let mut create_count = 0u64;
    let mut switched = false;
    println!("sec rpc_tip stream_slot stream_lag stream_age_ms pump_slot pump_lag pump_age_ms tx_per_sec creates");

    while started.elapsed() < Duration::from_secs(seconds) {
        tokio::select! {
            update = stream.next() => {
                let Some(update) = update else { anyhow::bail!("Geyser stream ended") };
                match update?.update_oneof {
                    Some(UpdateOneof::Slot(update)) => {
                        geyser_slot = Some((update.slot, Instant::now()));
                    }
                    Some(UpdateOneof::Transaction(update)) => {
                        pump_slot = Some((update.slot, Instant::now()));
                        transaction_count += 1;
                        let is_create = contains_create_instruction(&update);
                        create_count += u64::from(is_create);
                        if switch_to_mint && is_create && !switched {
                            if let Some(event) = decode_transactions(update.slot, &update)
                                .into_iter()
                                .find(|event| event.is_create)
                            {
                                let mut target_transactions = HashMap::new();
                                target_transactions.insert(
                                    "target".to_owned(),
                                    SubscribeRequestFilterTransactions {
                                        vote: Some(false),
                                        failed: Some(false),
                                        account_include: vec![event.mint.to_string()],
                                        ..Default::default()
                                    },
                                );
                                sink.send(SubscribeRequest {
                                    slots: HashMap::from([(
                                        "tip".to_owned(),
                                        SubscribeRequestFilterSlots {
                                            filter_by_commitment: Some(true),
                                            interslot_updates: Some(false),
                                        },
                                    )]),
                                    transactions: target_transactions,
                                    commitment: Some(CommitmentLevel::Processed as i32),
                                    from_slot: Some(update.slot),
                                    ..Default::default()
                                }).await?;
                                switched = true;
                                println!("switch target={} from_slot={}", event.mint, update.slot);
                            }
                        }
                    }
                    _ => {}
                }
            }
            _ = tick.tick() => {
                let Some(rpc_tip) = *rpc_tip.borrow() else { continue };
                let now = Instant::now();
                let format_sample = |sample: Option<(u64, Instant)>| match sample {
                    Some((slot, at)) => format!("{slot} {} {}", rpc_tip.saturating_sub(slot), now.duration_since(at).as_millis()),
                    None => "- - -".to_owned(),
                };
                println!(
                    "{} {} {} {} {} {}",
                    started.elapsed().as_secs(),
                    rpc_tip,
                    format_sample(geyser_slot),
                    format_sample(pump_slot),
                    transaction_count,
                    create_count,
                );
                transaction_count = 0;
                create_count = 0;
            }
        }
    }
    Ok(())
}

fn spawn_rpc_sampler(rpc: Arc<RpcClient>) -> watch::Receiver<Option<u64>> {
    let (tx, rx) = watch::channel(None);
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_millis(250));
        loop {
            interval.tick().await;
            let rpc = rpc.clone();
            if let Ok(Ok(slot)) = tokio::task::spawn_blocking(move || rpc.get_slot()).await {
                tx.send_replace(Some(slot));
            }
        }
    });
    rx
}
