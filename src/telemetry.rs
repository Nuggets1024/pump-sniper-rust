//! 日志与阶段耗时。
//!
//! 统一格式：
//! `12:00:01.123  INFO  [启动] 模式=scan  交易=是  监听=Pump发币`

use crate::config::LogCfg;
use std::fmt as stdfmt;
use std::fs;
use tracing::field::{Field, Visit};
use tracing::{Event, Subscriber};
use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::fmt::format::{Compact, Format, Writer};
use tracing_subscriber::fmt::time::ChronoLocal;
use tracing_subscriber::fmt::{FmtContext, FormatEvent, FormatFields};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::registry::LookupSpan;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{fmt, EnvFilter};

#[allow(dead_code)]
const STAGE_W: usize = 8;

pub fn init(cfg: &LogCfg) -> WorkerGuard {
    fs::create_dir_all(&cfg.directory).ok();
    let file_appender = tracing_appender::rolling::daily(&cfg.directory, "sniper.log");
    let (nb, guard) = tracing_appender::non_blocking(file_appender);

    let filter = EnvFilter::try_from_default_env()
        .or_else(|_| EnvFilter::try_new(&cfg.filter))
        .unwrap_or_else(|_| EnvFilter::new("info"));

    let stdout = fmt::layer().event_format(MarketAwareFormat::new(true));
    let file = fmt::layer()
        .event_format(MarketAwareFormat::new(false))
        .with_writer(nb);
    tracing_subscriber::registry()
        .with(filter)
        .with(stdout)
        .with(file)
        .init();
    guard
}

struct MarketAwareFormat {
    standard: Format<Compact, ChronoLocal>,
}

impl MarketAwareFormat {
    fn new(ansi: bool) -> Self {
        Self {
            standard: fmt::format()
                .with_timer(ChronoLocal::new("%H:%M:%S%.3f".into()))
                .with_target(false)
                .with_thread_ids(false)
                .with_ansi(ansi)
                .compact(),
        }
    }
}

impl<S, N> FormatEvent<S, N> for MarketAwareFormat
where
    S: Subscriber + for<'lookup> LookupSpan<'lookup>,
    N: for<'writer> FormatFields<'writer> + 'static,
{
    fn format_event(
        &self,
        ctx: &FmtContext<'_, S, N>,
        mut writer: Writer<'_>,
        event: &Event<'_>,
    ) -> stdfmt::Result {
        if event.metadata().target() == "pump_sniper::market" {
            let mut visitor = MarketLineVisitor::default();
            event.record(&mut visitor);
            if let Some(line) = visitor.line {
                return writeln!(writer, "{line}");
            }
        }
        self.standard.format_event(ctx, writer, event)
    }
}

#[derive(Default)]
struct MarketLineVisitor {
    line: Option<String>,
}

impl Visit for MarketLineVisitor {
    fn record_str(&mut self, field: &Field, value: &str) {
        if field.name() == "market_line" {
            self.line = Some(value.to_owned());
        }
    }

    fn record_debug(&mut self, _field: &Field, _value: &dyn stdfmt::Debug) {}
}

fn pad_chars(s: &str, width: usize) -> String {
    let n = s.chars().count();
    if n >= width {
        s.chars().take(width).collect()
    } else {
        format!("{s}{}", " ".repeat(width - n))
    }
}

fn line(tag: &str, body: &str) -> String {
    if body.is_empty() {
        format!("[{tag}]")
    } else {
        format!("[{tag}] {body}")
    }
}

/// `是` / `否`
pub fn yn(v: bool) -> &'static str {
    if v {
        "是"
    } else {
        "否"
    }
}

#[allow(dead_code)]
pub fn ok_cn(v: bool) -> &'static str {
    if v {
        "成功"
    } else {
        "失败"
    }
}

pub fn info(tag: &str, body: impl AsRef<str>) {
    let body = body.as_ref();
    crate::admin::emit_log("INFO", tag, body);
    tracing::info!("{}", line(tag, body));
}

pub fn warn(tag: &str, body: impl AsRef<str>) {
    let body = body.as_ref();
    crate::admin::emit_log("WARN", tag, body);
    tracing::warn!("{}", line(tag, body));
}

pub fn error(tag: &str, body: impl AsRef<str>) {
    let body = body.as_ref();
    crate::admin::emit_log("ERROR", tag, body);
    tracing::error!("{}", line(tag, body));
}

pub fn info_fields(tag: &str, fields: impl IntoIterator<Item = String>) {
    info(tag, bracket_fields(fields));
}

pub fn error_fields(tag: &str, fields: impl IntoIterator<Item = String>) {
    error(tag, bracket_fields(fields));
}

/// 市场流水已经包含 token 日志的完整时间戳和列格式，不再添加业务标签。
pub fn market(body: impl AsRef<str>) {
    let body = body.as_ref();
    tracing::info!(target: "pump_sniper::market", market_line = body);
}

pub fn compact_line(tag: &str, fields: impl IntoIterator<Item = String>) -> String {
    format!(
        "{} INFO {}\n",
        compact_timestamp(),
        line(tag, &bracket_fields(fields))
    )
}

fn bracket_fields(fields: impl IntoIterator<Item = String>) -> String {
    fields
        .into_iter()
        .map(|field| format!("[ {field} ]"))
        .collect::<Vec<_>>()
        .join(" ")
}

fn compact_timestamp() -> String {
    let offset = chrono::FixedOffset::east_opt(8 * 60 * 60).expect("valid UTC+8 offset");
    chrono::Utc::now()
        .with_timezone(&offset)
        .format("%H:%M:%S%.3f")
        .to_string()
}

#[allow(dead_code)]
pub fn timing(stage: &str, ok: bool, us: u64, extra: &str) {
    let extra = extra.trim();
    let body = if extra.is_empty() {
        format!("{}  {}  {:>7}µs", pad_chars(stage, STAGE_W), ok_cn(ok), us)
    } else {
        format!(
            "{}  {}  {:>7}µs  {}",
            pad_chars(stage, STAGE_W),
            ok_cn(ok),
            us,
            extra
        )
    };
    info("耗时", body);
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io;
    use std::sync::{Arc, Mutex};

    #[test]
    fn log_tags_are_not_padded_or_truncated() {
        let chinese = line("交易", "body");
        let ascii = line("CREATE", "body");
        assert_eq!(chinese, "[交易] body");
        assert_eq!(ascii, "[CREATE] body");
        assert!(ascii.contains("CREATE"));
    }

    #[test]
    fn market_event_is_written_verbatim() {
        #[derive(Clone)]
        struct Buffer(Arc<Mutex<Vec<u8>>>);

        impl io::Write for Buffer {
            fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
                self.0.lock().unwrap().extend_from_slice(bytes);
                Ok(bytes.len())
            }

            fn flush(&mut self) -> io::Result<()> {
                Ok(())
            }
        }

        let bytes = Arc::new(Mutex::new(Vec::new()));
        let writer = {
            let bytes = bytes.clone();
            move || Buffer(bytes.clone())
        };
        let subscriber = tracing_subscriber::registry().with(
            fmt::layer()
                .event_format(MarketAwareFormat::new(false))
                .with_writer(writer),
        );
        let line = "[ 09-01 16:17:45.729 ] [ 买入 ] [ wallet ] [ PNL 0% ]";
        tracing::subscriber::with_default(subscriber, || market(line));

        assert_eq!(
            String::from_utf8(bytes.lock().unwrap().clone()).unwrap(),
            format!("{line}\n")
        );
    }
}
