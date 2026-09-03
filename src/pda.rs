//! Pump 相关 PDA 推导。

use crate::constants::*;
use solana_sdk::pubkey::Pubkey;
use spl_associated_token_account::get_associated_token_address_with_program_id;

pub fn bonding_curve(mint: &Pubkey) -> Pubkey {
    Pubkey::find_program_address(&[b"bonding-curve", mint.as_ref()], &PUMP_PROGRAM_ID).0
}

pub fn bonding_curve_v2(mint: &Pubkey) -> Pubkey {
    Pubkey::find_program_address(&[b"bonding-curve-v2", mint.as_ref()], &PUMP_PROGRAM_ID).0
}

pub fn creator_vault(creator: &Pubkey) -> Pubkey {
    Pubkey::find_program_address(&[b"creator-vault", creator.as_ref()], &PUMP_PROGRAM_ID).0
}

pub fn user_volume_accumulator(user: &Pubkey) -> Pubkey {
    Pubkey::find_program_address(
        &[b"user_volume_accumulator", user.as_ref()],
        &PUMP_PROGRAM_ID,
    )
    .0
}

pub fn associated_bonding_curve(mint: &Pubkey, curve: &Pubkey, token_program: &Pubkey) -> Pubkey {
    get_associated_token_address_with_program_id(curve, mint, token_program)
}

pub fn associated_user(owner: &Pubkey, mint: &Pubkey, token_program: &Pubkey) -> Pubkey {
    get_associated_token_address_with_program_id(owner, mint, token_program)
}

pub fn seed_token_account(owner: &Pubkey, seed: &str, token_program: &Pubkey) -> Pubkey {
    Pubkey::create_with_seed(owner, seed, token_program).expect("seed 账户")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    #[test]
    fn global_matches_known() {
        let (pda, _) = Pubkey::find_program_address(&[b"global"], &PUMP_PROGRAM_ID);
        assert_eq!(pda, *PUMP_GLOBAL_ID);
    }

    #[test]
    fn event_authority_matches_known() {
        let (pda, _) = Pubkey::find_program_address(&[b"__event_authority"], &PUMP_PROGRAM_ID);
        assert_eq!(pda, *EVENT_AUTHORITY_ID);
    }

    #[test]
    fn curve_is_off_curve() {
        let mint = Pubkey::from_str("5jqSjpoPNAnKNg4YL7mdGvMbZgMnjA947drDYB1Zpump").unwrap();
        let curve = bonding_curve(&mint);
        assert_ne!(curve, mint);
    }

    #[test]
    fn bonding_curve_v2_matches_mainnet_buy_fixture() {
        let mint = Pubkey::from_str("Fi8qz7xjoh5Wjfc4CKsHAvRAqrGrvDGt4nCKHN8dpump").unwrap();
        let expected = Pubkey::from_str("2mvEiTwqGQRp6GBoJMyfc3xbz7nX83oVod9QiyTZ5Mzp").unwrap();
        assert_eq!(bonding_curve_v2(&mint), expected);
    }
}
