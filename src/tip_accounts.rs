//! 各落地供应商公开的完整 tip 钱包池。

use crate::config::LandingProvider;

pub const ZERO_SLOT: &[&str] = &[
    "6fQaVhYZA4w3MBSXjJ81Vf6W1EDYeUPXpgVQ6UQyU1Av",
    "4HiwLEP2Bzqj3hM2ENxJuzhcPCdsafwiet3oGkMkuQY4",
    "7toBU3inhmrARGngC7z6SjyP85HgGMmCTEwGNRAcYnEK",
    "8mR3wB1nh4D6J9RUCugxUpc6ya8w38LPxZ3ZjcBhgzws",
    "6SiVU5WEwqfFapRuYCndomztEwDjvS5xgtEof3PLEGm9",
    "TpdxgNJBWZRL8UXF5mrEsyWxDWx9HQexA9P1eTWQ42p",
    "D8f3WkQu6dCF33cZxuAsrKHrGsqGP2yvAHf8mX6RXnwf",
    "GQPFicsy3P3NXxB5piJohoxACqTvWE9fKpLgdsMduoHE",
    "Ey2JEr8hDkgN8qKJGrLf2yFjRhW7rab99HVxwi5rcvJE",
    "4iUgjMT8q2hNZnLuhpqZ1QtiV8deFPy2ajvvjEpKKgsS",
    "3Rz8uD83QsU8wKvZbgWAPvCNDU6Fy8TSZTMcPm3RB6zt",
    "DiTmWENJsHQdawVUUKnUXkconcpW4Jv52TnMWhkncF6t",
    "HRyRhQ86t3H4aAtgvHVpUJmw64BDrb61gRiKcdKUXs5c",
    "7y4whZmw388w1ggjToDLSBLv47drw5SUXcLk6jtmwixd",
    "J9BMEWFbCBEjtQ1fG5Lo9kouX1HfrKQxeUxetwXrifBw",
    "8U1JPQh3mVQ4F5jwRdFTBzvNRQaYFQppHQYoH38DJGSQ",
    "Eb2KpSC8uMt9GmzyAEm5Eb1AAAgTjRaXWFjKyFXHZxF3",
    "FCjUJZ1qozm1e8romw216qyfQMaaWKxWsuySnumVCCNe",
    "ENxTEjSQ1YabmUpXAdCgevnHQ9MHdLv8tzFiuiYJqa13",
    "6rYLG55Q9RpsPGvqdPNJs4z5WTxJVatMB8zV3WJhs5EK",
    "Cix2bHfqPcKcM233mzxbLk14kSggUUiz2A87fJtGivXr",
];

pub const TEMPORAL_ALL: &[&str] = &[
    "TEMPaMeCRFAS9EKF53Jd6KpHxgL47uWLcpFArU1Fanq",
    "noz3jAjPiHuBPqiSPkkugaJDkJscPuRhYnSpbi8UvC4",
    "noz3str9KXfpKknefHji8L1mPgimezaiUyCHYMDv1GE",
    "noz6uoYCDijhu1V7cutCpwxNiSovEwLdRHPwmgCGDNo",
    "noz9EPNcT7WH6Sou3sr3GGjHQYVkN3DNirpbvDkv9YJ",
    "nozc5yT15LazbLTFVZzoNZCwjh3yUtW86LoUyqsBu4L",
    "nozFrhfnNGoyqwVuwPAW4aaGqempx4PU6g6D9CJMv7Z",
    "nozievPk7HyK1Rqy1MPJwVQ7qQg2QoJGyP71oeDwbsu",
    "noznbgwYnBLDHu8wcQVCEw6kDrXkPdKkydGJGNXGvL7",
    "nozNVWs5N8mgzuD3qigrCG2UoKxZttxzZ85pvAQVrbP",
    "nozpEGbwx4BcGp6pvEdAh1JoC2CQGZdU6HbNP1v2p6P",
    "nozrhjhkCr3zXT3BiT4WCodYCUFeQvcdUkM7MqhKqge",
    "nozrwQtWhEdrA6W8dkbt9gnUaMs52PdAv5byipnadq3",
    "nozUacTVWub3cL4mJmGCYjKZTnE9RbdY5AP46iQgbPJ",
    "nozWCyTPppJjRuw2fpzDhhWbW355fzosWSzrrMYB1Qk",
    "nozWNju6dY353eMkMqURqwQEoM3SFgEKC6psLCSfUne",
    "nozxNBgWohjR75vdspfxR5H9ceC7XXH99xpxhVGt3Bb",
];

pub const ASTRALANE: &[&str] = &[
    "astrazznxsGUhWShqgNtAdfrzP2G83DzcWVJDxwV9bF",
    "astra4uejePWneqNaJKuFFA8oonqCE1sqF6b45kDMZm",
    "astra9xWY93QyfG6yM8zwsKsRodscjQ2uU2HKNL5prk",
    "astraRVUuTHjpwEVvNBeQEgwYx9w9CFyfxjYoobCZhL",
    "astraEJ2fEj8Xmy6KLG7B3VfbKfsHXhHrNdCQx7iGJK",
    "astraubkDw81n4LuutzSQ8uzHCv4BhPVhfvTcYv8SKC",
    "astraZW5GLFefxNPAatceHhYjfA1ciq9gvfEg2S47xk",
    "astrawVNP4xDBKT7rAdxrLYiTSTdqtUr63fSMduivXK",
    "AstrA1ejL4UeXC2SBP4cpeEmtcFPZVLxx3XGKXyCW6to",
    "AsTra79FET4aCKWspPqeSFvjJNyp96SvAnrmyAxqg5b7",
    "AstrABAu8CBTyuPXpV4eSCJ5fePEPnxN8NqBaPKQ9fHR",
    "AsTRADtvb6tTmrsqULQ9Wji9PigDMjhfEMza6zkynEvV",
    "AsTRAEoyMofR3vUPpf9k68Gsfb6ymTZttEtsAbv8Bk4d",
    "AStrAJv2RN2hKCHxwUMtqmSxgdcNZbihCwc1mCSnG83W",
    "Astran35aiQUF57XZsmkWMtNCtXGLzs8upfiqXxth2bz",
    "AStRAnpi6kFrKypragExgeRoJ1QnKH7pbSjLAKQVWUum",
    "ASTRaoF93eYt73TYvwtsv6fMWHWbGmMUZfVZPo3CRU9C",
];

pub const LANDX: &[&str] = &[
    "LandX1EMgYtrfi4jVJ7ryby4mDpZcef6sVH9b9U1cog",
    "LandX2gHE7y6RejuvoLJK4hJiKkrAwcrJJ34u19Gmo1",
    "LandX3uK1cSUF7hbefKo3oL1j4FhvhxitiMRKut99Z6",
    "LandX4wXzvDyWfq1XXn4StRjwPjzqb7je2kqDGLoT9L",
    "LandX5T4jfRASrcn76owxHM4q9bduNGj24ev4Ufc5fP",
    "LandX6RsjjnfaxfYAFTZX1TW9Cgfe9HFStj9nRDd5SK",
    "LandX7LsdzXDAbbyBjAq5FxLTnGQaU3LojGQMLZCvkt",
    "LandX8Gv3UkhjmgKdSEg4F7yG7Z2tiqhNDYDtxazydN",
    "LandX9fhNN5S7fuBgekKBYeYvK1Sy9hbGvDmHsBGYFh",
    "LandXXxDXaSS8MjrqG9nri51htrfM6V4R3zXDVCeV8R",
];

pub fn for_provider(provider: LandingProvider) -> &'static [&'static str] {
    match provider {
        LandingProvider::Rpc => &[],
        LandingProvider::ZeroSlot => ZERO_SLOT,
        LandingProvider::Temporal => TEMPORAL_ALL,
        LandingProvider::Astralane => ASTRALANE,
        LandingProvider::Landx => LANDX,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    #[test]
    fn public_pools_are_complete_and_valid() {
        assert_eq!(ZERO_SLOT.len(), 21);
        assert_eq!(TEMPORAL_ALL.len(), 17);
        assert_eq!(ASTRALANE.len(), 17);
        assert_eq!(LANDX.len(), 10);
        for account in ZERO_SLOT
            .iter()
            .chain(TEMPORAL_ALL)
            .chain(ASTRALANE)
            .chain(LANDX)
        {
            solana_sdk::pubkey::Pubkey::from_str(account).unwrap();
        }
    }
}
