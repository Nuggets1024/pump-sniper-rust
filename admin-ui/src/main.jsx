import React, { useEffect, useMemo, useState } from "react";
import { createRoot } from "react-dom/client";
import {
  Button, Card, CardBody, CardHeader, Chip, Divider, HeroUIProvider, Link,
  Navbar, NavbarBrand, NavbarContent, NavbarItem, ScrollShadow, Spinner, Table,
  TableBody, TableCell, TableColumn, TableHeader, TableRow, Tooltip
} from "@heroui/react";
import {
  Activity, Bot, ChevronDown, ChevronUp, CircleDollarSign, DatabaseZap,
  ExternalLink, Pause, Play, Radio, RefreshCw, Search, Server, Wallet
} from "lucide-react";
import "./styles.css";

const token = localStorage.getItem("pumpSniperAdminToken") || "";
const authHeaders = token ? { Authorization: `Bearer ${token}` } : {};

async function api(path, options = {}) {
  const response = await fetch(path, {
    ...options,
    headers: { ...authHeaders, ...(options.headers || {}) }
  });
  if (response.status === 401) {
    const entered = prompt("Admin token");
    if (entered) localStorage.setItem("pumpSniperAdminToken", entered);
    location.reload();
  }
  return response;
}

function shortHash(value, start = 6, end = 4) {
  if (!value) return "-";
  return value.length > start + end + 3
    ? `${value.slice(0, start)}…${value.slice(-end)}`
    : value;
}

function price(value) {
  if (value == null || !Number.isFinite(Number(value))) return "-";
  return Number(value).toPrecision(6);
}

function profitPercent(item) {
  const buy = Number(item.buy_price);
  const sell = Number(item.sell_price);
  if (!Number.isFinite(buy) || !Number.isFinite(sell) || buy === 0) return null;
  return ((sell - buy) / buy) * 100;
}

function statusColor(status) {
  const value = (status || "").toLowerCase();
  if (value.includes("失败") || value.includes("fail") || value.includes("错误")) return "danger";
  if (value.includes("成功") || value.includes("success") || value.includes("已卖")) return "success";
  if (value.includes("等待") || value.includes("待确认") || value.includes("pending")) return "warning";
  return "default";
}

function StatusItem({ icon: Icon, label, value, color, href }) {
  return (
    <div className="min-w-0 px-4 py-3">
      <div className="mb-1 flex items-center gap-1.5 text-tiny text-default-500">
        <Icon size={14} />
        <span>{label}</span>
      </div>
      {color ? (
        <Chip color={color} variant="dot" size="sm" className="border-0 px-0 font-medium">{value || "-"}</Chip>
      ) : href ? (
        <Tooltip content={String(value || "-")} delay={500}>
          <Link isExternal href={href} color="foreground" className="block max-w-full truncate text-small font-semibold">
            {value || "-"}
          </Link>
        </Tooltip>
      ) : (
        <Tooltip content={String(value || "-")} delay={500}>
          <p className="truncate text-small font-semibold tabular-nums">{value || "-"}</p>
        </Tooltip>
      )}
    </div>
  );
}

function HashLink({ value, label }) {
  if (!value) return <span className="text-default-400">{label} -</span>;
  return (
    <Tooltip content={value} delay={450}>
      <Link
        isExternal showAnchorIcon anchorIcon={<ExternalLink size={11} />}
        href={`https://solscan.io/tx/${value}`} size="sm"
        className="hash-link font-mono text-[11px]"
      >
        {label} {shortHash(value, 5, 4)}
      </Link>
    </Tooltip>
  );
}

function PanelTitle({ title, description, endContent }) {
  return (
    <CardHeader className="flex min-h-12 items-center justify-between gap-4 px-4 py-2.5">
      <div className="min-w-0">
        <h2 className="text-small font-semibold leading-5">{title}</h2>
        <p className="truncate text-tiny text-default-400">{description}</p>
      </div>
      {endContent}
    </CardHeader>
  );
}

function App() {
  const [status, setStatus] = useState(null);
  const [tokens, setTokens] = useState([]);
  const [events, setEvents] = useState([]);
  const [holdings, setHoldings] = useState([]);
  const [holdingBusy, setHoldingBusy] = useState(false);
  const [refreshing, setRefreshing] = useState(false);
  const [commandBusy, setCommandBusy] = useState("");
  const [logsOpen, setLogsOpen] = useState(true);

  async function refresh(showBusy = false) {
    if (showBusy) setRefreshing(true);
    try {
      const [statusResponse, tokensResponse] = await Promise.all([
        api("/api/status"), api("/api/tokens")
      ]);
      const nextStatus = await statusResponse.json();
      setStatus(nextStatus);
      setTokens(await tokensResponse.json());
      setEvents((items) => items.length ? items : (nextStatus.recent_logs || []));
    } finally {
      if (showBusy) setRefreshing(false);
    }
  }

  async function command(path) {
    setCommandBusy(path);
    try {
      const response = await api(path, { method: "POST" });
      if (!response.ok) alert(await response.text());
      await refresh();
    } finally {
      setCommandBusy("");
    }
  }

  async function fetchHoldings(mints = []) {
    setHoldingBusy(true);
    try {
      const response = await api("/api/holdings", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ mints: mints ?? [] })
      });
      if (!response.ok) {
        alert(await response.text());
        return;
      }
      setHoldings(await response.json());
    } finally {
      setHoldingBusy(false);
    }
  }

  useEffect(() => {
    refresh();
    fetchHoldings();
    const timer = setInterval(refresh, 1000);
    const eventsUrl = `/api/events${token ? `?token=${encodeURIComponent(token)}` : ""}`;
    const stream = new EventSource(eventsUrl);
    stream.onmessage = (event) => {
      setEvents((items) => [...items.slice(-399), JSON.parse(event.data)]);
    };
    return () => {
      clearInterval(timer);
      stream.close();
    };
  }, []);

  const activeHoldings = useMemo(
    () => holdings.filter((item) => item.amount !== "0" || item.error), [holdings]
  );
  const newestEvents = useMemo(() => [...events].reverse(), [events]);
  const isRunning = status?.bot === "running";
  const logUrl = (mint) => `/api/tokens/${encodeURIComponent(mint)}/logs${token ? `?token=${encodeURIComponent(token)}` : ""}`;

  return (
    <div className="min-h-screen bg-background text-foreground">
      <Navbar maxWidth="full" isBordered className="line-navbar"
        classNames={{ wrapper: "h-14 px-5 xl:px-8" }}>
        <NavbarBrand className="gap-3">
          <div className="grid size-8 place-items-center rounded-medium border border-divider text-default-700">
            <Bot size={17} strokeWidth={1.75} />
          </div>
          <div>
            <p className="text-small font-semibold leading-4 tracking-tight">Pump Sniper</p>
            <p className="text-[10px] text-default-400">交易控制台</p>
          </div>
        </NavbarBrand>
        <NavbarContent justify="end" className="gap-2">
          <NavbarItem>
            <Button size="sm" color="success" variant="solid" startContent={<Play size={14} />}
              isLoading={commandBusy === "/api/bot/start"} isDisabled={isRunning || !!commandBusy}
              onPress={() => command("/api/bot/start")}>
              {isRunning ? "已启动" : "启动"}
            </Button>
          </NavbarItem>
          <NavbarItem>
            <Button size="sm" color="danger" variant="solid" startContent={<Pause size={14} />}
              isLoading={commandBusy === "/api/bot/stop"} isDisabled={!isRunning || !!commandBusy}
              onPress={() => command("/api/bot/stop")}>
              停止并卖出
            </Button>
          </NavbarItem>
          <NavbarItem>
            <Tooltip content="立即刷新">
              <Button isIconOnly size="sm" variant="light" aria-label="立即刷新"
                isLoading={refreshing} onPress={() => refresh(true)}>
                <RefreshCw size={16} />
              </Button>
            </Tooltip>
          </NavbarItem>
        </NavbarContent>
      </Navbar>

      <main className="grid w-full gap-3 px-5 py-3 xl:px-8">
        <Card shadow="none" className="line-panel">
          <CardBody className="grid grid-cols-2 divide-x divide-divider p-0 md:grid-cols-3 xl:grid-cols-6">
            <StatusItem icon={Activity} label="Bot" value={isRunning ? "运行中" : "已停止"}
              color={isRunning ? "success" : "danger"} />
            <StatusItem icon={CircleDollarSign} label="余额"
              value={status?.balance_lamports == null ? "-" : `${(status.balance_lamports / 1e9).toFixed(6)} SOL`} />
            <StatusItem icon={DatabaseZap} label="最新 Slot" value={status?.latest_slot} />
            <StatusItem icon={Radio} label="连接" value={`${status?.geyser_connections ?? "-"} Geyser`} />
            <StatusItem icon={Wallet} label="钱包" value={shortHash(status?.wallet, 9, 7)}
              href={status?.wallet ? `https://gmgn.ai/sol/address/${status.wallet}` : undefined} />
            <StatusItem icon={Server} label="节点" value={status?.geyser_endpoint} />
          </CardBody>
        </Card>

        <Card shadow="none" className="line-panel">
          <CardBody className="flex min-h-14 flex-col justify-center gap-3 px-4 py-2.5 md:flex-row md:items-center">
            <div className="flex shrink-0 items-center gap-2">
              <Wallet size={16} className="text-default-500" />
              <span className="text-small font-semibold">实时持仓</span>
            </div>
            {activeHoldings.length > 0 ? (
              <ScrollShadow orientation="horizontal" className="flex min-w-0 flex-1 gap-2 py-1">
                {activeHoldings.map((item) => (
                  <Chip key={`${item.mint}-${item.token_program}`}
                    color={item.error ? "danger" : "success"} variant="flat" className="shrink-0">
                    {shortHash(item.mint)} · {item.error ? "ERR" : item.ui_amount_string}
                  </Chip>
                ))}
              </ScrollShadow>
            ) : <p className="min-w-0 flex-1 text-small text-default-400">暂无持仓</p>}
            <Button size="sm" color="primary" variant="solid" startContent={!holdingBusy && <Search size={14} />}
              isLoading={holdingBusy} onPress={() => fetchHoldings()}>
              刷新持仓
            </Button>
          </CardBody>
        </Card>

        <Card shadow="none" className="line-panel min-w-0">
          <PanelTitle title="狙击列表"
            description="成功签名计算 Slot 间隔，失败签名不会覆盖成功记录"
            endContent={<Chip size="sm" variant="bordered">{tokens.length} 条记录</Chip>} />
          <Divider />
          <Table removeWrapper aria-label="狙击记录" className="snipe-table-shell"
            classNames={{ table: "snipe-table", th: "snipe-th", td: "snipe-td" }}>
            <TableHeader>
              <TableColumn width={146} align="center">时间</TableColumn>
              <TableColumn width={100} align="center">Token</TableColumn>
              <TableColumn width={100} align="center">狙击状态</TableColumn>
              <TableColumn width={82} align="center">间隔 Slot</TableColumn>
              <TableColumn width={100} align="center">买入价格 / 卖出价格</TableColumn>
              <TableColumn width={86} align="center">利润</TableColumn>
              <TableColumn width={238} align="center">Dev Hash / 狙击 Hash</TableColumn>
              <TableColumn width={140} align="center">通道</TableColumn>
              <TableColumn width={160} align="center">备注</TableColumn>
              <TableColumn width={72} align="center">操作</TableColumn>
            </TableHeader>
            <TableBody emptyContent="暂无狙击记录" isLoading={!status}
              loadingContent={<Spinner label="正在加载" />}>
              {tokens.map((item) => {
                const profit = profitPercent(item);
                return (
                  <TableRow key={item.mint}>
                    <TableCell><span className="whitespace-nowrap font-mono text-[11px]">{item.create_time || "-"}</span></TableCell>
                    <TableCell>
                      <Tooltip content={item.mint} delay={450}>
                        <Link isExternal showAnchorIcon anchorIcon={<ExternalLink size={11} />}
                          href={`https://gmgn.ai/sol/token/${item.mint}`} size="sm"
                          className="font-mono text-[11px]">{shortHash(item.mint)}</Link>
                      </Tooltip>
                    </TableCell>
                    <TableCell>
                      <Chip size="sm" color={statusColor(item.status)} variant="flat" className="max-w-full">
                        <span className="block max-w-[86px] truncate">{item.status || "未知"}</span>
                      </Chip>
                    </TableCell>
                    <TableCell><span className="tabular-nums">{item.interval_slot ?? "-"}</span></TableCell>
                    <TableCell>
                      <Tooltip content={`买入 ${price(item.buy_price)} / 卖出 ${price(item.sell_price)}`} delay={450}>
                        <span className="whitespace-nowrap font-mono text-[11px] tabular-nums">
                          <span className="text-default-400">买</span> {price(item.buy_price)}
                          <span className="mx-1.5 text-divider">/</span>
                          <span className="text-default-400">卖</span> {price(item.sell_price)}
                        </span>
                      </Tooltip>
                    </TableCell>
                    <TableCell>
                      {profit == null ? <span className="text-default-400">-</span> : (
                        <Chip size="sm" color={profit >= 0 ? "success" : "danger"}
                          variant="flat" className="font-mono text-[11px]">
                          {profit >= 0 ? "+" : ""}{profit.toFixed(2)}%
                        </Chip>
                      )}
                    </TableCell>
                    <TableCell>
                      <div className="flex items-center justify-center gap-2 whitespace-nowrap">
                        <HashLink value={item.dev_hash} label="Dev" />
                                <span className="text-divider">/</span>
                        <HashLink value={item.sniper_hash} label="狙击" />
                      </div>
                    </TableCell>
                    <TableCell><span className="block truncate text-small">{item.execution_channel || "-"}</span></TableCell>
                    <TableCell>
                      <Tooltip content={item.remark || "无备注"} delay={500}>
                        <span className="block truncate text-small text-default-600">{item.remark || "-"}</span>
                      </Tooltip>
                    </TableCell>
                    <TableCell>
                      <Button as="a" isIconOnly size="sm" variant="light" color="primary"
                        href={logUrl(item.mint)} target="_blank" rel="noreferrer" aria-label="打开原始日志">
                        <ExternalLink size={14} />
                      </Button>
                    </TableCell>
                  </TableRow>
                );
              })}
            </TableBody>
          </Table>
        </Card>

        <Card shadow="none" className="line-panel min-w-0">
          <PanelTitle title="实时操作日志"
            description="最新事件置顶，保留最近 400 条"
            endContent={
              <div className="flex items-center gap-2">
                <Chip size="sm" color="success" variant="dot">Live</Chip>
                <Button size="sm" variant="light"
                  startContent={logsOpen ? <ChevronUp size={14} /> : <ChevronDown size={14} />}
                  aria-expanded={logsOpen} aria-controls="live-log-stream"
                  onPress={() => setLogsOpen((open) => !open)}>
                  {logsOpen ? "收起" : "展开"}
                </Button>
              </div>
            } />
          {logsOpen && (
            <>
              <Divider />
              <ScrollShadow id="live-log-stream" className="h-[460px]">
                <div className="log-stream" role="log" aria-live="polite">
                  {newestEvents.length ? newestEvents.map((event, index) => (
                    <div className="log-entry" key={`${event.ts}-${events.length - index}`}>
                      <span className="log-time">{event.ts}</span>
                      <Chip size="sm" variant="flat"
                        color={event.level === "ERROR" ? "danger" : event.level === "WARN" ? "warning" : "default"}
                        className="h-5 w-fit font-mono text-[10px]">{event.level}</Chip>
                      <strong className="log-tag">{event.tag}</strong>
                      <span className="log-message">{event.message}</span>
                    </div>
                  )) : <div className="px-4 py-10 text-center text-small text-default-400">等待实时日志…</div>}
                </div>
              </ScrollShadow>
            </>
          )}
        </Card>
      </main>
    </div>
  );
}

createRoot(document.getElementById("root")).render(
  <HeroUIProvider><App /></HeroUIProvider>
);
