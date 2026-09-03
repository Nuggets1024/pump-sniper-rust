//! 链上程序 ID、指令 discriminator 与手续费地址。

use solana_sdk::pubkey::Pubkey;
use std::str::FromStr;
use std::sync::LazyLock;

pub const PUMP_PROGRAM: &str = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P";
pub const PUMP_AMM_PROGRAM: &str = "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA";
pub const PUMP_FEE_PROGRAM: &str = "pfeeUxB6jkeY1Hxd7CsFCAjcbHA9rWtchMGdZ6VojVZ";
pub const PUMP_GLOBAL: &str = "4wTV1YmiEkRvAtNtsSGPtUrqRYQMe5SKy2uB4Jjaxnjf";
pub const EVENT_AUTHORITY: &str = "Ce6TQqeHC9p8KetsN6JsjHK7UTZk7nasjjnr7XxXp9F1";
pub const FEE_CONFIG: &str = "8Wf5TiAheLUqBrKXeYg2JtAFFMWtKdG2BSFgqUcPVwTt";
pub const GLOBAL_VOLUME_ACCUMULATOR: &str = "Hq2wp8uJ9jCPsYgNHex8RtqdvMPfVGoYwjvF1ATiwn2Y";
pub const LIGHTHOUSE: &str = "L2TExMFKdjpN9kozasaurPirfHy9P8sbXoAN1qA3S95";
pub const JITO_DONT_FRONT: &str = "jitodontfront111111111111111111111111111111";
/// Pump `[b"mint-authority"]` PDA；create/create_v2 专属账户。
pub const PUMP_MINT_AUTHORITY: &str = "TSLvdd1pWpHVjahSpsvCXUbgwsL3JAcvokwaKt1eokM";

pub const IX_CREATE: [u8; 8] = [24, 30, 200, 40, 5, 28, 7, 119];
pub const IX_CREATE_V2: [u8; 8] = [214, 144, 76, 236, 95, 139, 49, 180];
pub const IX_BUY: [u8; 8] = [102, 6, 61, 18, 1, 218, 235, 234];
pub const IX_BUY_EXACT_SOL_IN: [u8; 8] = [56, 252, 116, 8, 158, 223, 205, 95];
pub const IX_BUY_V2: [u8; 8] = [184, 23, 238, 97, 103, 197, 211, 61];
pub const IX_BUY_EXACT_QUOTE_IN_V2: [u8; 8] = [194, 171, 28, 70, 104, 77, 91, 47];
pub const IX_SELL: [u8; 8] = [51, 230, 133, 164, 1, 127, 131, 173];
pub const IX_SELL_V2: [u8; 8] = [93, 246, 130, 60, 231, 233, 64, 178];
pub const IX_AMM_BUY_EXACT_QUOTE_IN: [u8; 8] = [198, 46, 21, 82, 180, 217, 232, 112];

/// 仅用于估算 min_tokens_out 的初始虚拟储备，不是实时报价。
pub const INIT_VIRTUAL_SOL: u64 = 30_000_000_000;
pub const INIT_VIRTUAL_TOKEN: u64 = 1_073_000_191_000_000;

pub static PUMP_PROGRAM_ID: LazyLock<Pubkey> =
    LazyLock::new(|| Pubkey::from_str(PUMP_PROGRAM).unwrap());
pub static PUMP_AMM_PROGRAM_ID: LazyLock<Pubkey> =
    LazyLock::new(|| Pubkey::from_str(PUMP_AMM_PROGRAM).unwrap());
pub static PUMP_FEE_PROGRAM_ID: LazyLock<Pubkey> =
    LazyLock::new(|| Pubkey::from_str(PUMP_FEE_PROGRAM).unwrap());
pub static PUMP_GLOBAL_ID: LazyLock<Pubkey> =
    LazyLock::new(|| Pubkey::from_str(PUMP_GLOBAL).unwrap());
pub static EVENT_AUTHORITY_ID: LazyLock<Pubkey> =
    LazyLock::new(|| Pubkey::from_str(EVENT_AUTHORITY).unwrap());
pub static FEE_CONFIG_ID: LazyLock<Pubkey> =
    LazyLock::new(|| Pubkey::from_str(FEE_CONFIG).unwrap());
pub static GLOBAL_VOLUME_ACCUMULATOR_ID: LazyLock<Pubkey> =
    LazyLock::new(|| Pubkey::from_str(GLOBAL_VOLUME_ACCUMULATOR).unwrap());
pub static LIGHTHOUSE_ID: LazyLock<Pubkey> =
    LazyLock::new(|| Pubkey::from_str(LIGHTHOUSE).unwrap());
pub static JITO_DONT_FRONT_ID: LazyLock<Pubkey> =
    LazyLock::new(|| Pubkey::from_str(JITO_DONT_FRONT).unwrap());

/// Jito `getTipAccounts` 返回的 8 个固定小费账户。
pub static JITO_TIP_ACCOUNTS: LazyLock<[Pubkey; 8]> = LazyLock::new(|| {
    [
        Pubkey::from_str("96gYZGLnJYVFmbjzopPSU6QiEV5fGqZNyN9nmNhvrZU5").unwrap(),
        Pubkey::from_str("HFqU5x63VTqvQss8hp11i4wVV8bD44PvwucfZ2bU7gRe").unwrap(),
        Pubkey::from_str("Cw8CFyM9FkoMi7K7Crf6HNQqf4uEMzpKw6QNghXLvLkY").unwrap(),
        Pubkey::from_str("ADaUMid9yfUytqMBgopwjb2DTLSokTSzL1zt6iGPaS49").unwrap(),
        Pubkey::from_str("DfXygSm4jCyNCybVYYK6DwvWqjKee8pbDmJGcLWNDXjh").unwrap(),
        Pubkey::from_str("ADuUkR4vqLUMWXxW9gh6D6L8pMSawimctcNZ5pGwDcEt").unwrap(),
        Pubkey::from_str("DttWaMuVvTiduZRnguLF7jNxTgiMBZ1hyAumKUiL2KRL").unwrap(),
        Pubkey::from_str("3AVi9Tg9Uo68tJfuvoKvqKNWKkC5wPdSSdeBnizKZ6jT").unwrap(),
    ]
});

pub static PROTOCOL_FEE_RECIPIENTS: LazyLock<[Pubkey; 2]> = LazyLock::new(|| {
    [
        Pubkey::from_str("CebN5WGQ4jvEPvsVU4EoHEpgzq1VV7AbicfhtW4xC9iM").unwrap(),
        Pubkey::from_str("62qc2CNXwrYqQScmEdiZFFAnJR262PxWEuNQtxfafNgV").unwrap(),
    ]
});

pub static BUYBACK_FEE_RECIPIENTS: LazyLock<[Pubkey; 8]> = LazyLock::new(|| {
    [
        Pubkey::from_str("5YxQFdt3Tr9zJLvkFccqXVUwhdTWJQc1fFg2YPbxvxeD").unwrap(),
        Pubkey::from_str("9M4giFFMxmFGXtc3feFzRai56WbBqehoSeRE5GK7gf7").unwrap(),
        Pubkey::from_str("GXPFM2caqTtQYC2cJ5yJRi9VDkpsYZXzYdwYpGnLmtDL").unwrap(),
        Pubkey::from_str("3BpXnfJaUTiwXnJNe7Ej1rcbzqTTQUvLShZaWazebsVR").unwrap(),
        Pubkey::from_str("5cjcW9wExnJJiqgLjq7DEG75Pm6JBgE1hNv4B2vHXUW6").unwrap(),
        Pubkey::from_str("EHAAiTxcdDwQ3U4bU6YcMsQGaekdzLS3B5SmYo46kJtL").unwrap(),
        Pubkey::from_str("5eHhjP8JaYkz83CWwvGU2uMUXefd3AazWGx4gpcuEEYD").unwrap(),
        Pubkey::from_str("A7hAgCzFw14fejgCp387JUJRMNyz4j89JKnhtKU8piqW").unwrap(),
    ]
});

pub fn pick_protocol_fee() -> Pubkey {
    PROTOCOL_FEE_RECIPIENTS[rand::random::<usize>() % PROTOCOL_FEE_RECIPIENTS.len()]
}

pub fn pick_buyback_fee_recipient() -> Pubkey {
    BUYBACK_FEE_RECIPIENTS[rand::random::<usize>() % BUYBACK_FEE_RECIPIENTS.len()]
}
