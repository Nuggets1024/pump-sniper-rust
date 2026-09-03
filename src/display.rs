//! 交易日志使用的紧凑数值格式。

use crate::pump::format_raw_amount;
use solana_sdk::pubkey::Pubkey;
use std::sync::atomic::{AtomicU64, Ordering};

const TOKEN_DECIMALS: f64 = 1_000_000.0;
const TOKEN_SUPPLY: f64 = 1_000_000_000.0;
static SOL_USD_BITS: AtomicU64 = AtomicU64::new(0);

pub fn set_sol_usd(price: f64) {
    SOL_USD_BITS.store(price.to_bits(), Ordering::Relaxed);
}

pub fn sol_usd() -> Option<f64> {
    let bits = SOL_USD_BITS.load(Ordering::Relaxed);
    (bits != 0).then(|| f64::from_bits(bits))
}

pub fn compact_token(raw_amount: u64) -> String {
    let value = raw_amount as f64 / TOKEN_DECIMALS;
    if value.abs() >= 1_000_000.0 {
        format!("{}M", trim_fixed(value / 1_000_000.0, 1))
    } else if value.abs() >= 1_000.0 {
        format!("{}K", trim_fixed(value / 1_000.0, 1))
    } else {
        trim_fixed(value, 2)
    }
}

pub fn compact_quote(raw_amount: u64, decimals: Option<u8>) -> String {
    compact_decimal(&format_raw_amount(raw_amount, decimals))
}

pub fn compact_market_cap(
    virtual_quote_reserves: Option<u64>,
    quote_mint: Pubkey,
    quote_decimals: Option<u8>,
    virtual_token_reserves: Option<u64>,
) -> String {
    compact_market_cap_with_sol_usd(
        virtual_quote_reserves,
        quote_mint,
        quote_decimals,
        virtual_token_reserves,
        sol_usd(),
    )
}

fn compact_market_cap_with_sol_usd(
    virtual_quote_reserves: Option<u64>,
    quote_mint: Pubkey,
    quote_decimals: Option<u8>,
    virtual_token_reserves: Option<u64>,
    sol_usd: Option<f64>,
) -> String {
    let Some(quote) = virtual_quote_reserves else {
        return "未知".into();
    };
    let Some(tokens) = virtual_token_reserves.filter(|amount| *amount > 0) else {
        return "未知".into();
    };
    let Some(quote_decimals) = quote_decimals else {
        return "未知".into();
    };
    let quote_value = quote as f64 / 10f64.powi(i32::from(quote_decimals));
    let price = quote_value / (tokens as f64 / TOKEN_DECIMALS);
    let quote_market_cap = price * TOKEN_SUPPLY;
    let usd_market_cap = if crate::pump::is_native_quote(quote_mint) {
        let Some(sol_usd) = sol_usd else {
            return "未知".into();
        };
        quote_market_cap * sol_usd
    } else if quote_mint.to_string() == "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v" {
        quote_market_cap
    } else {
        return "未知".into();
    };
    compact_scaled(usd_market_cap)
}

fn compact_scaled(value: f64) -> String {
    let (scaled, suffix) = if value.abs() >= 1_000_000.0 {
        (value / 1_000_000.0, "M")
    } else if value.abs() >= 1_000.0 {
        (value / 1_000.0, "K")
    } else {
        (value, "")
    };
    let decimals = if scaled.abs() >= 100.0 {
        0
    } else if scaled.abs() >= 10.0 {
        1
    } else {
        2
    };
    format!("{}{suffix}", trim_fixed(scaled, decimals))
}

fn compact_decimal(value: &str) -> String {
    let Some((whole, fraction)) = value.split_once('.') else {
        return value.to_owned();
    };
    if whole != "0" {
        return trim_significant(value, 3);
    }
    let zeros = fraction.bytes().take_while(|byte| *byte == b'0').count();
    if zeros < 3 {
        let fraction = fraction.chars().take(3).collect::<String>();
        let value = format!("0.{fraction}");
        return value.trim_end_matches('0').trim_end_matches('.').to_owned();
    }
    let significant = fraction[zeros..].chars().take(3).collect::<String>();
    format!("0.0{}{}", subscript(zeros), significant)
}

fn trim_significant(value: &str, digits: usize) -> String {
    let parsed = value.parse::<f64>().unwrap_or(0.0);
    if parsed == 0.0 {
        return "0".into();
    }
    let decimals = (digits as i32 - 1 - parsed.abs().log10().floor() as i32).max(0) as usize;
    trim_fixed(parsed, decimals)
}

fn trim_fixed(value: f64, decimals: usize) -> String {
    let mut result = format!("{value:.decimals$}");
    if result.contains('.') {
        while result.ends_with('0') {
            result.pop();
        }
        if result.ends_with('.') {
            result.pop();
        }
    }
    result
}

fn subscript(value: usize) -> String {
    value
        .to_string()
        .chars()
        .map(|digit| match digit {
            '0' => '₀',
            '1' => '₁',
            '2' => '₂',
            '3' => '₃',
            '4' => '₄',
            '5' => '₅',
            '6' => '₆',
            '7' => '₇',
            '8' => '₈',
            '9' => '₉',
            _ => unreachable!(),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn formats_requested_trade_amount_examples() {
        assert_eq!(compact_token(34_300_000_000_000), "34.3M");
        assert_eq!(compact_token(33_100_000_000), "33.1K");
        assert_eq!(compact_token(9_000_000), "9");
        assert_eq!(compact_quote(989_000_000, Some(9)), "0.989");
        assert_eq!(compact_quote(9_999_999, Some(9)), "0.009");
        assert_eq!(compact_quote(980_000, Some(9)), "0.0₃98");
        assert_eq!(compact_quote(268, Some(9)), "0.0₆268");
    }

    #[test]
    fn formats_market_cap_with_k_and_m_suffixes() {
        assert_eq!(compact_scaled(3_080.0), "3.08K");
        assert_eq!(compact_scaled(3_080_000.0), "3.08M");
    }

    #[test]
    fn formats_real_sell_market_cap_in_usd() {
        assert_eq!(
            compact_market_cap_with_sol_usd(
                Some(30_000_000_001),
                spl_token::native_mint::ID,
                Some(9),
                Some(1_073_000_000_000_000),
                Some(106.69527509486761),
            ),
            "2.98K"
        );
    }
}
