use pump_sniper::constants::IX_BUY_EXACT_SOL_IN;
use pump_sniper::pda;
use pump_sniper::pump::ix::{buy_exact_sol_in, min_tokens_out, naive_min_tokens, BuyAccounts};
use solana_sdk::pubkey::Pubkey;
use std::str::FromStr;

#[test]
fn buy_exact_sol_in_matches_current_idl_shape() {
    let mint = Pubkey::from_str("5jqSjpoPNAnKNg4YL7mdGvMbZgMnjA947drDYB1Zpump").unwrap();
    let user = Pubkey::new_unique();
    let curve = pda::bonding_curve(&mint);
    let ix = buy_exact_sol_in(
        &BuyAccounts {
            mint,
            bonding_curve: curve,
            associated_bonding_curve: Pubkey::new_unique(),
            user,
            user_token: Pubkey::new_unique(),
            creator_vault: pda::creator_vault(&user),
            user_volume: pda::user_volume_accumulator(&user),
            token_program: spl_token_2022::ID,
            fee_recipient: Pubkey::new_unique(),
            bonding_curve_v2: pda::bonding_curve_v2(&mint),
            buyback_fee_recipient: Pubkey::new_unique(),
        },
        903_625_081,
        3_891_848_418_044,
    );
    assert_eq!(ix.data.len(), 25);
    assert_eq!(&ix.data[..8], &IX_BUY_EXACT_SOL_IN);
    assert_eq!(ix.data[24], 1);
    assert_eq!(ix.accounts.len(), 18);
    assert_eq!(ix.accounts[16].pubkey, pda::bonding_curve_v2(&mint));
}

#[test]
fn min_out_is_below_naive_quote() {
    let min = naive_min_tokens(1_000_000_000, 0.3);
    assert!(min > 0);
    assert!(min < naive_min_tokens(1_000_000_000, 1.0));
}

#[test]
fn min_out_uses_live_reserves_and_integer_slippage_bps() {
    // 10 * 1000 / (10 + 10) = 500，无滑点报价；10% 保护后为 450。
    assert_eq!(min_tokens_out(10, 10, 1000, 1000), Some(450));
    assert_eq!(min_tokens_out(10, 10, 1000, 10_000), None);
}
