use crate::config::AppConfig;
use crate::constants::{pick_buyback_fee_recipient, pick_protocol_fee, JITO_DONT_FRONT_ID};
use crate::exec::land::{LandTransaction, Lander};
use crate::pda;
use crate::position::Position;
use crate::pump::ix::sell;
use anyhow::Context;
use solana_sdk::compute_budget::ComputeBudgetInstruction;
use solana_sdk::hash::Hash;
use solana_sdk::instruction::AccountMeta;
use solana_sdk::message::Message;
use solana_sdk::signature::{Keypair, Signer};
use solana_sdk::transaction::Transaction;
use spl_token_2022::instruction::close_account;

pub struct SellResult {
    pub signature: String,
    pub signatures: Vec<String>,
    pub channel: String,
}

pub async fn execute_sell(
    cfg: &AppConfig,
    lander: &Lander,
    payer: &Keypair,
    pos: &Position,
    recent: Hash,
) -> anyhow::Result<SellResult> {
    let user = payer.pubkey();
    let assoc_curve =
        pda::associated_bonding_curve(&pos.mint, &pos.bonding_curve, &pos.token_program);
    let bonding_curve_v2 = pda::bonding_curve_v2(&pos.mint);
    let creator_vault = pda::creator_vault(&pos.creator);

    let mut ixs = vec![
        {
            let mut limit = ComputeBudgetInstruction::set_compute_unit_limit(cfg.landing.cu_limit);
            if cfg.landing.jito_dont_front {
                limit
                    .accounts
                    .push(AccountMeta::new_readonly(*JITO_DONT_FRONT_ID, false));
            }
            limit
        },
        ComputeBudgetInstruction::set_compute_unit_price(cfg.landing.cu_price_micro_lamports),
        sell(
            pos.mint,
            pos.bonding_curve,
            assoc_curve,
            user,
            pos.user_token,
            creator_vault,
            pos.token_program,
            pos.token_amount,
            cfg.sell.min_sol_out_lamports,
            pick_protocol_fee(),
            bonding_curve_v2,
            pick_buyback_fee_recipient(),
        ),
    ];
    if let Ok(close) = close_account(&pos.token_program, &pos.user_token, &user, &user, &[]) {
        ixs.push(close);
    }
    let transactions = lander
        .tip_plans(&user)
        .into_iter()
        .map(|tip| {
            let mut ixs = ixs.clone();
            ixs.extend(tip.instructions.iter().cloned());
            let msg = Message::new(&ixs, Some(&user));
            let mut transaction = Transaction::new_unsigned(msg);
            transaction.try_sign(&[payer], recent).context("卖出签名")?;
            Ok(LandTransaction { transaction, tip })
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    let receipt = lander.send_any(transactions).await?;
    Ok(SellResult {
        signature: receipt.signature.to_string(),
        signatures: receipt.signatures.iter().map(ToString::to_string).collect(),
        channel: receipt.channel,
    })
}
