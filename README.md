# pump-sniper

跟 Pump.fun 发盘钱包（dev）的 Rust 狙击程序。

热路径：**Yellowstone gRPC → 解码 `create` / `create_v2` → `BuyExactSolIn`（18 账户，Token-2022）→ 多通道并行上链**。

模块严格分为：**监控产生市场事实 → 策略产生 Intent → 交易负责状态、签名和执行**。详细接口见 [`docs/architecture.md`](docs/architecture.md)。

`scan` 用于循环测试狙击，`standard` 用于长期监听/跟盘。市场流水、订单审计和管理页实时日志始终记录，示例配置默认关闭交易。

## 做什么

- 用 Yellowstone（processed）监听 Pump create/create_v2
- `follow` 非空时只接受指定 dev 的发币，dev 后续普通 buy/sell 不作为跟买信号
- 把 `create` / `create_v2`（可带同笔 buy）当成狙击信号
- 解码并展示 token 名称、symbol、URI、mint、dev、同笔买入 SOL 参数与 Jito/bundle 线索
- 为每个 mint 异步记录 `logs/<mint>.log`：DEV 创建、所有买卖钱包、实际成交 SOL/token、slot 与交易/指令顺序
- 每条买卖流水展示当前价格、本钱包剩余持仓移动平均成本价和 PNL 百分比；未持仓时成本与 PNL 均为 0
- 组现网账户：creator vault、成交量累加器、fee 程序、trailing fee、可选 Lighthouse 卡 slot、Jito `dontfront`
- Token-2022 用 `createAccountWithSeed` 建临时账户（不是经典 ATA）
- 卖出：只跟同一 creator 卖，或到 `sell.max_hold_ms` 强制出；原子状态机避免重复卖单
- 多 endpoint 同时发送同一份已签交易，首个 RPC 接受即返回，其他通道继续发送
- 交易提交使用可插拔 adapter，内置普通 RPC、0slot、Temporal/Nozomi、Astralane、LandX；LandX 可切换 HTTP/UDP
- 内置各供应商完整公开 tip 钱包池，每笔交易随机选择，避免固定钱包写锁竞争
- 买卖均等待 Geyser 观察到本钱包同签名交易后才最终变更仓位；买入使用精确 Pump 成交事件的 token 数量（尚不是账户余额 delta）
- Geyser 只配置一个 endpoint；程序内部固定开两条连接：主连接监听 CREATE/follow，目标连接监听当前 mint 交易
- 内置本地管理页：钱包、余额、最新 slot、狙击结果列表、实时操作日志、配置文件编辑；命令行启动后默认暂停，需在页面点“启动”才开始记录和交易
- `standard` 模式为主 feed 持久化 slot cursor；slot stream 缺口和服务端 replay 越界会进入持久 RPC block repair 队列
- `events.wal.jsonl` 持久化记录事件；`standard` 的 cursor/pending gap 可跨重启恢复，`scan` 每次启动自动归零
- 每阶段打耗时微秒：`连接Geyser`、`解码`、`策略`、`组买入`、`上链`、`超时卖出` / `跟卖`

这一版还没接 Shredstream。Geyser 是第一条数据面；Shred 以后可以往同一条 `PumpEvent` 通道丢，不是二选一。

## 启动

需要 Rust 1.89+（本机用 stable）。

```bash
cp config.example.toml config.toml
solana-keygen new -o keypair.json --no-bip39-passphrase
chmod 600 keypair.json config.toml
# 填写 rpc.url、geyser.endpoint 和 follow[].address
cargo build --release
./target/release/pump-sniper --config config.toml
```

密钥文件必须是 `600` 权限，否则进程拒绝启动。

启动后默认打开本地管理端口：

```text
http://127.0.0.1:8787
```

部署到服务器时建议保持 `admin.bind = "127.0.0.1:8787"`，从本机用 SSH 隧道访问：

```bash
ssh -L 8787:127.0.0.1:8787 user@your-server-ip
```

## 配置

见 `config.example.toml`。

| 段 | 说明 |
|---|---|
| `bot.mode` | `standard` 长期监听/跟盘；`scan` 锁定首个可买 create，之后只处理该 mint |
| `bot.trade` | 是否真实执行买卖；关闭时为纯监听，`scan` 必须开启 |
| `admin.enabled` | 是否启动内置管理页 |
| `admin.bind` | 管理页监听地址；服务器部署建议只绑定 `127.0.0.1` |
| `admin.auth_token` | 管理页 Bearer token；公网反代前必须配置长随机值 |
| `geyser.endpoint` | 唯一 Geyser endpoint；程序内部复用它开主监听和 mint 监听两条连接 |
| `geyser.x_token` | Geyser 鉴权 token，可留空 |
| `follow` | 发盘钱包；`min_sol` / `max_sol` 卡 create 同笔 dev 买入 |
| `buy.sol_amount` | `BuyExactSolIn` 花费的 SOL |
| `landing.routes` | 并行提交 route；不同供应商 tip 会生成不同交易 variant，谁先接受谁算成功 |
| `landing.routes[].transport` | `http` 或 `udp`；当前只有 LandX 支持 UDP |
| `landing.routes[].tip_accounts` | 可选 private/custom 钱包池；空时使用内置完整公开池 |
| `landing.max_total_tip_lamports` | 一笔交易允许的所有启用 route tip 总预算 |
| `landing.max_inflight_orders` | 跨 mint 最大并发订单数 |
| `landing.confirmation_timeout_ms` | RPC 接受后等待 Geyser 同签名确认的时限；超时熔断新开仓 |
| `landing.allow_insecure_http` | 是否允许 LandX 明文 HTTP；默认 false，API key 位于 URL path |
| `landing.lighthouse_slot_guard` | 错过 slot 窗口则交易失败；默认容许 12 slots 的落地延迟 |
| `buy.max_slippage_bps` | 整数滑点上限；优先按事件实时虚拟储备计算 `min_tokens_out` |
| `buy.use_seed_token_account` | 多 route 交易必须开启；所有 variant 共用同一个非幂等临时 token account 防重复买入 |
| `log.directory` | 总日志及每 mint 交易流水目录 |

配置只保留一套入口：Geyser 统一使用 `endpoint`，交易提交统一使用 `routes`，滑点统一使用
`max_slippage_bps`。未知字段会导致启动失败，避免字段拼错后静默回退默认值。

管理页的“配置”标签会直接读取和保存 `config.toml`；保存前会按同一套配置校验。点击“重载校验”
只会确认文件可用并更新页面提示，Geyser/RPC/钱包/落地 route 这类连接配置仍需停止后重启进程才完整生效。

主日志和每 mint 的 token 日志共用同一套 CREATE/买入/卖出格式化结果，包括完整钱包、
成交金额、token 数量、MCAP、当前价/成本价、PNL、slot、身份和签名。任何日志都不写私钥。

## 日志格式

终端和 `logs/sniper.log` 同一套对齐格式（每天滚动）：

```
12:00:01.123  INFO  [启动] 模式=scan  交易=是  监听=Pump发币
12:00:01.124  INFO  [钱包] BDuA…t1My
12:00:01.500  INFO  [订阅] Geyser
12:00:01.501  INFO  [引擎] 钱包=...  模式=scan  监听=Pump发币  跟盘=0
12:00:02.001  INFO  [耗时] 解码        成功      180µs  <签名>
12:00:02.002  INFO  [耗时] 策略        成功       12µs
12:00:02.010  INFO  [耗时] 组买入      成功      420µs  <mint>
12:00:02.011  INFO  [并发提交] sig=<签名>  routes=3  [0slot,temporal,landx]  bytes=1180
12:00:02.013  INFO  [提交结果] sig=<签名>  route=landx  status=dispatched  elapsed=180µs
12:00:02.018  INFO  [提交结果] sig=<签名>  route=0slot  status=accepted  elapsed=6500µs
12:00:02.018  INFO  [提交完成] sig=<签名>  winner=0slot  elapsed=6700µs
12:00:02.020  INFO  [耗时] 上链        成功     9000µs  <交易签名>
12:00:02.021  INFO  [跳过] 金额区间  mint=...  sol=0.020  范围=0.1-5
```

标签统一按终端显示宽度补齐：`启动` `钱包` `订阅` `引擎` `CREATE` `交易` `重连` `错误`。  
搜 `[耗时]` 看延迟。上链最长 = 落地慢；解码最长 = Geyser 包体大。

每个 token 还会生成 `logs/<mint>.log`。买入顺序按
`slot → Geyser transaction_index → Pump instruction_index` 排列；SOL/token 优先取 Pump
`TradeEvent` 的实际成交值，缺失时会明确标记为“指令参数回退”。示例：

```text
[ 09-01 16:17:45.729 ] [ 买入 ] [ <wallet> ] [ 0.009 SOL   ] [ 342.1K   ] [ MCAP 2.89K    ] [ 0.00000002816/0             ] [ PNL 0%     ] [    443376152 ] [ 其他     ] [ <signature> ]
```

`scan` 模式示例：

```toml
[bot]
mode = "scan"
trade = true
```

配置了 `[[follow]]` 时，监听和买入信号只来自这些钱包；没有配置 `follow` 时自动监听
Pump 发币。`scan` 不能配置 `follow`：它用第一个可买的实时 create 作为目标，锁定期间其他 mint
不再进入 journal 和交易引擎。卖出确认后目标连接继续监听该 mint 30 秒，然后移除目标并回到等待下一个
create。`scan` 每次启动都清空 cursor 和 pending gap，只接当前实时流，不做历史重放或 RPC 回补。
主 Geyser 连接始终保留 create/follow 监听；目标连接在 standby 与 target mint 订阅之间切换。

```text
[ 08-31 18:14:05.451 ] [ 卖出 ] [ <wallet> ] [ 0.029 SOL ] [ 3.2M ] [ MCAP 540 ] [ 0.00000001696/0.00000002696 ] [ PNL -37% ] [ 443125947 ] [ DEV ] [ <signature> ]
```

“RPC已接受”只代表落地节点接收了交易；只有 Geyser 后续观察到同一签名时才写
“确认交易/上链”。同一份已签名交易发往多个节点时签名应当相同，日志不会为不同通道伪造签名。
如果为了不同 tip/CU price 构建不同交易，它们可能全部成交；当前落地器不会自动生成这类多签名变体。

市值和当前价格优先按该笔 Pump `TradeEvent` 的虚拟储备计算；储备缺失时当前价格回退到
该笔实际成交均价。成本价按本钱包剩余持仓的移动平均成本计算，`PNL=(当前价/成本价-1)×100%`。
本钱包尚未买入或已经清仓时，成本价和 PNL 均显示 0。

当前解码范围包含 Pump bonding-curve 的全部公开买卖指令（包括 V2）和
PumpSwap 的 `buy` / `buy_exact_quote_in` / `sell`。PumpSwap 及非 SOL 报价币目前只监听统计，
自动下单仍只支持 bonding-curve + SOL；其他 DEX 仍不在范围内。

`bundle_id` 无法从普通 Geyser 交易更新中取得；日志会保留 Jito tip、`dontfront`
等线索，并明确写 `bundle_id=Geyser不可得`，不会把线索误报成已确认 bundle。

## 安全

- 示例配置默认关闭 `bot.trade`
- 密钥 `chmod 600`
- mint / creator 黑名单
- Lighthouse 限制 slot，避免过期 nonce 买在高位
- 计算预算指令挂 `jito_dont_front`，降低被夹概率
- 多 route 会按供应商 tip 生成不同签名 variant；买入强制共用同一个 `createAccountWithSeed` 临时账户，用链上账户冲突防重复成交
- durable nonce 在正确实现官方 nonce state 校验和并发租约前保持禁用
- 审计 journal 溢出或链上确认超时会熔断新开仓，但不会阻止已有仓位退出
- `standard` 中 cursor/WAL 写入失败、多源冲突或 pending gap 未回补完成时，停止新开仓但继续确认和平仓
- 发送前拒绝超过 1232 bytes 的 wire transaction

当前确认超时采用 fail-closed：永久停止本进程的新开仓，等待重放确认或人工处置。
RPC 余额差核账和订单级持久 reconcile queue 尚未实现，因此当前版本仍不宣称端到端“绝不漏交易”。

这不是投资建议。发盘盘口对抗性强，进同一 slot 只是门槛，不是稳赚。
