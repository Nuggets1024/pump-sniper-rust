use crate::constants::*;
use crate::pda;
use solana_sdk::instruction::{AccountMeta, Instruction};
use solana_sdk::pubkey::Pubkey;
use solana_system_interface::instruction as system_instruction;
use solana_sysvar::clock as sysvar_clock;

pub struct BuyAccounts {
    pub mint: Pubkey,
    pub bonding_curve: Pubkey,
    pub associated_bonding_curve: Pubkey,
    pub user: Pubkey,
    pub user_token: Pubkey,
    pub creator_vault: Pubkey,
    pub user_volume: Pubkey,
    pub token_program: Pubkey,
    pub fee_recipient: Pubkey,
    pub bonding_curve_v2: Pubkey,
    pub buyback_fee_recipient: Pubkey,
}

pub fn buy_exact_sol_in(
    accounts: &BuyAccounts,
    spendable_sol: u64,
    min_tokens: u64,
) -> Instruction {
    let mut data = Vec::with_capacity(25);
    data.extend_from_slice(&IX_BUY_EXACT_SOL_IN);
    data.extend_from_slice(&spendable_sol.to_le_bytes());
    data.extend_from_slice(&min_tokens.to_le_bytes());
    data.push(1); // track_volume = true

    Instruction {
        program_id: *PUMP_PROGRAM_ID,
        accounts: vec![
            AccountMeta::new_readonly(*PUMP_GLOBAL_ID, false),
            AccountMeta::new(accounts.fee_recipient, false),
            AccountMeta::new_readonly(accounts.mint, false),
            AccountMeta::new(accounts.bonding_curve, false),
            AccountMeta::new(accounts.associated_bonding_curve, false),
            AccountMeta::new(accounts.user_token, false),
            AccountMeta::new(accounts.user, true),
            AccountMeta::new_readonly(solana_system_interface::program::ID, false),
            AccountMeta::new_readonly(accounts.token_program, false),
            AccountMeta::new(accounts.creator_vault, false),
            AccountMeta::new_readonly(*EVENT_AUTHORITY_ID, false),
            AccountMeta::new_readonly(*PUMP_PROGRAM_ID, false),
            AccountMeta::new_readonly(*GLOBAL_VOLUME_ACCUMULATOR_ID, false),
            AccountMeta::new(accounts.user_volume, false),
            AccountMeta::new_readonly(*FEE_CONFIG_ID, false),
            AccountMeta::new_readonly(*PUMP_FEE_PROGRAM_ID, false),
            AccountMeta::new_readonly(accounts.bonding_curve_v2, false),
            AccountMeta::new(accounts.buyback_fee_recipient, false),
        ],
        data,
    }
}

pub fn sell(
    mint: Pubkey,
    curve: Pubkey,
    assoc_curve: Pubkey,
    user: Pubkey,
    user_token: Pubkey,
    creator_vault: Pubkey,
    token_program: Pubkey,
    amount: u64,
    min_sol: u64,
    fee_recipient: Pubkey,
    user_volume: Pubkey,
    bonding_curve_v2: Pubkey,
    buyback_fee_recipient: Pubkey,
) -> Instruction {
    let mut data = Vec::with_capacity(24);
    data.extend_from_slice(&IX_SELL);
    data.extend_from_slice(&amount.to_le_bytes());
    data.extend_from_slice(&min_sol.to_le_bytes());
    Instruction {
        program_id: *PUMP_PROGRAM_ID,
        accounts: vec![
            AccountMeta::new_readonly(*PUMP_GLOBAL_ID, false),
            AccountMeta::new(fee_recipient, false),
            AccountMeta::new_readonly(mint, false),
            AccountMeta::new(curve, false),
            AccountMeta::new(assoc_curve, false),
            AccountMeta::new(user_token, false),
            AccountMeta::new(user, true),
            AccountMeta::new_readonly(solana_system_interface::program::ID, false),
            AccountMeta::new(creator_vault, false),
            AccountMeta::new_readonly(token_program, false),
            AccountMeta::new_readonly(*EVENT_AUTHORITY_ID, false),
            AccountMeta::new_readonly(*PUMP_PROGRAM_ID, false),
            AccountMeta::new_readonly(*FEE_CONFIG_ID, false),
            AccountMeta::new_readonly(*PUMP_FEE_PROGRAM_ID, false),
            // Cashback coins interpret the first remaining account as the
            // seller's writable UserVolumeAccumulator PDA. It must precede
            // bonding_curve_v2; otherwise that PDA is rejected with 6073.
            AccountMeta::new(user_volume, false),
            AccountMeta::new_readonly(bonding_curve_v2, false),
            AccountMeta::new(buyback_fee_recipient, false),
        ],
        data,
    }
}

/// Token / Token-2022：createAccountWithSeed + InitializeAccount3。
pub fn token_account_setup(
    payer: &Pubkey,
    seed: &str,
    mint: &Pubkey,
    token_program: &Pubkey,
) -> anyhow::Result<(Pubkey, Vec<Instruction>)> {
    let account = pda::seed_token_account(payer, seed, token_program);
    let lamports = 2_039_280; // token 账户免租余额
    let create = system_instruction::create_account_with_seed(
        payer,
        &account,
        payer,
        seed,
        lamports,
        165,
        token_program,
    );
    let init =
        spl_token_2022::instruction::initialize_account3(token_program, &account, mint, payer)?;
    Ok((account, vec![create, init]))
}

/// Lighthouse AssertSysvarClock：当前 slot <= `slot`。
/// 编码对齐现网狙击交易（变体 15，Slot，LessThanOrEqual）。
pub fn lighthouse_slot_leq(slot: u64) -> Instruction {
    let mut data = Vec::with_capacity(12);
    data.push(0x0f); // AssertSysvarClock
    data.push(0x00); // 日志级别 Silent
    data.push(0x00); // ClockAssertion::Slot
    data.extend_from_slice(&slot.to_le_bytes());
    data.push(0x05); // IntegerOperator::LessThanOrEqual
    Instruction {
        program_id: *LIGHTHOUSE_ID,
        accounts: vec![AccountMeta::new_readonly(sysvar_clock::ID, false)],
        data,
    }
}

pub fn naive_min_tokens(spendable_sol: u64, ratio: f64) -> u64 {
    // 代币 ≈ sol * 虚拟代币 / (虚拟 SOL + sol)
    let vt = INIT_VIRTUAL_TOKEN as u128;
    let vs = INIT_VIRTUAL_SOL as u128;
    let sol = spendable_sol as u128;
    let out = sol.saturating_mul(vt) / vs.saturating_add(sol);
    ((out as f64) * ratio).max(1.0) as u64
}

/// 按当前虚拟储备和整数滑点计算最少到手量。
pub fn min_tokens_out(
    spendable_quote: u64,
    virtual_quote_reserves: u64,
    virtual_token_reserves: u64,
    max_slippage_bps: u16,
) -> Option<u64> {
    if spendable_quote == 0
        || virtual_quote_reserves == 0
        || virtual_token_reserves == 0
        || max_slippage_bps >= 10_000
    {
        return None;
    }
    let quote = u128::from(spendable_quote);
    let virtual_quote = u128::from(virtual_quote_reserves);
    let virtual_token = u128::from(virtual_token_reserves);
    let expected = quote
        .checked_mul(virtual_token)?
        .checked_div(virtual_quote.checked_add(quote)?)?;
    let protected = expected
        .checked_mul(u128::from(10_000u16 - max_slippage_bps))?
        .checked_div(10_000)?;
    u64::try_from(protected.max(1)).ok()
}
