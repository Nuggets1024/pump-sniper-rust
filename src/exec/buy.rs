use crate::config::AppConfig;
use crate::constants::{pick_buyback_fee_recipient, pick_protocol_fee, JITO_DONT_FRONT_ID};
use crate::exec::land::{LandTransaction, Lander};
use crate::pda;
use crate::pump::ix::{
    buy_exact_sol_in, lighthouse_slot_leq, min_tokens_out, token_account_setup, BuyAccounts,
};
use crate::pump::PumpEvent;
use anyhow::Context;
use solana_sdk::compute_budget::ComputeBudgetInstruction;
use solana_sdk::hash::Hash;
use solana_sdk::instruction::AccountMeta;
use solana_sdk::instruction::Instruction;
use solana_sdk::message::Message;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::{Keypair, Signer};
use solana_sdk::transaction::Transaction;
use std::time::{SystemTime, UNIX_EPOCH};

pub struct BuyResult {
    pub signature: String,
    pub signatures: Vec<String>,
    pub channel: String,
    pub user_token: Pubkey,
    pub tokens_est: u64,
}

pub async fn execute_buy(
    cfg: &AppConfig,
    lander: &Lander,
    payer: &Keypair,
    ev: &PumpEvent,
    recent: Hash,
    slot: u64,
) -> anyhow::Result<BuyResult> {
    anyhow::ensure!(
        crate::pump::is_native_quote(ev.quote_mint),
        "当前下单器仅支持 SOL 报价币，收到 quote={}",
        ev.quote_mint
    );
    let user = payer.pubkey();
    let curve = ev.bonding_curve;
    let assoc_curve = pda::associated_bonding_curve(&ev.mint, &curve, &ev.token_program);
    let bonding_curve_v2 = pda::bonding_curve_v2(&ev.mint);
    let creator_vault = pda::creator_vault(&ev.creator);
    let user_volume = pda::user_volume_accumulator(&user);

    let seed = unique_seed(ev.slot, &ev.mint);
    let (user_token, mut ixs) = if cfg.buy.use_seed_token_account {
        token_account_setup(&user, &seed, &ev.mint, &ev.token_program)?
    } else {
        let ata = pda::associated_user(&user, &ev.mint, &ev.token_program);
        let ix =
            spl_associated_token_account::instruction::create_associated_token_account_idempotent(
                &user,
                &user,
                &ev.mint,
                &ev.token_program,
            );
        (ata, vec![ix])
    };

    let spendable = cfg.buy_lamports();
    let (virtual_quote, virtual_token) = live_reserves(ev).unwrap_or((
        crate::constants::INIT_VIRTUAL_SOL,
        crate::constants::INIT_VIRTUAL_TOKEN,
    ));
    let min_out = min_tokens_out(
        spendable,
        virtual_quote,
        virtual_token,
        cfg.buy_max_slippage_bps(),
    )
    .context("按虚拟储备计算 min_tokens_out")?;
    let buy_ix = buy_exact_sol_in(
        &BuyAccounts {
            mint: ev.mint,
            bonding_curve: curve,
            associated_bonding_curve: assoc_curve,
            user,
            user_token,
            creator_vault,
            user_volume,
            token_program: ev.token_program,
            fee_recipient: pick_protocol_fee(),
            bonding_curve_v2,
            buyback_fee_recipient: pick_buyback_fee_recipient(),
        },
        spendable,
        min_out,
    );

    let mut prefix = budget_ixs(cfg);
    if cfg.landing.lighthouse_slot_guard {
        prefix.push(lighthouse_slot_leq(
            slot.saturating_add(cfg.landing.lighthouse_slot_slack),
        ));
    }
    prefix.append(&mut ixs);
    prefix.push(buy_ix);
    let transactions = lander
        .tip_plans(&user)
        .into_iter()
        .map(|tip| {
            let mut ixs = prefix.clone();
            ixs.extend(tip.instructions.iter().cloned());
            let msg = Message::new(&ixs, Some(&user));
            let mut transaction = Transaction::new_unsigned(msg);
            transaction.try_sign(&[payer], recent).context("买入签名")?;
            Ok(LandTransaction { transaction, tip })
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    let receipt = lander.send_any(transactions).await?;
    Ok(BuyResult {
        signature: receipt.signature.to_string(),
        signatures: receipt.signatures.iter().map(ToString::to_string).collect(),
        channel: receipt.channel,
        user_token,
        tokens_est: min_out,
    })
}

fn live_reserves(ev: &PumpEvent) -> Option<(u64, u64)> {
    ev.buys
        .iter()
        .rev()
        .find_map(|buy| Some((buy.virtual_quote_reserves?, buy.virtual_token_reserves?)))
        .or_else(|| {
            ev.sells
                .iter()
                .rev()
                .find_map(|sell| Some((sell.virtual_quote_reserves?, sell.virtual_token_reserves?)))
        })
        .filter(|(quote, token)| *quote > 0 && *token > 0)
}

fn budget_ixs(cfg: &AppConfig) -> Vec<Instruction> {
    let mut limit = ComputeBudgetInstruction::set_compute_unit_limit(cfg.landing.cu_limit);
    if cfg.landing.jito_dont_front {
        limit
            .accounts
            .push(AccountMeta::new_readonly(*JITO_DONT_FRONT_ID, false));
    }
    vec![
        limit,
        ComputeBudgetInstruction::set_compute_unit_price(cfg.landing.cu_price_micro_lamports),
    ]
}

fn unique_seed(slot: u64, mint: &Pubkey) -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    // seed 最长 32 字节
    let raw = format!("{:x}{:x}", slot ^ (nanos as u64), mint.to_bytes()[0]);
    raw.chars().take(32).collect()
}
