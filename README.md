# Reliable Webhook Delivery API

一个本地可运行的 Webhook 可靠投递服务：接收带 **目标端点** 和 **幂等键** 的事件，
校验 HMAC-SHA256 签名后入队，由后台调度器按顺序、带退避重试地向目标端点投递；
三次重试仍失败则进入死信。技术栈：**Python · FastAPI · SQLite**。

## 特性

- **HMAC-SHA256 签名校验**：签名针对**原始请求体字节**计算（`X-Webhook-Signature: sha256=<hex>`），密钥来自环境变量 `WEBHOOK_SECRET`，使用恒定时间比较。
- **幂等接收**：`idempotency_key` 在数据库中有唯一约束。重复提交返回**原事件**（HTTP 200，响应头 `Idempotent-Replay: true`），**不会再次入队或投递**。
- **投递时间窗**：创建事件可携带 RFC 3339 的 `deliver_after`（默认为创建时刻）与 `expires_at`（可选，必须晚于 `deliver_after`，否则 422）。调度器不会在 `deliver_after` 之前投递；尚未到时的前序事件依旧按创建顺序阻塞同目标后续事件。每次领取前，调度器在**同一事务**内把已过 `expires_at` 且仍为 `queued` 的事件标记为 `expired`——不产生任何投递尝试，也不再阻塞同目标的后续事件；已进入 `processing` 的事件不受到期影响，会正常投递完成。
- **同一目标端点严格有序**：以创建顺序（SQLite `rowid`）为准，前一条事件未成功或进入死信前，后一条不会被取出投递。不同端点之间并行投递，互不阻塞。
- **重试与死信**：首次立即投递；失败后分别等待 **1s / 2s / 4s** 重试（共 4 次尝试）；第 4 次仍失败（非 2xx、超时、连接错误）即进入死信（状态 `dead`）。
- **死信重放**：`POST /events/{event_id}/replay` 仅接受 `dead` 事件，在一个事务内将其重新入队并开启**新一轮完整**的 1s/2s/4s 退避（再次享有 4 次尝试）。历史 `attempts` 全部保留、`attempt_no` 连续递增，`replay_count` 累计并记录 `last_replayed_at`，幂等键与事件 id 不变；并发重复重放只会生效一次，不会重复投递。
- **事件查询**：返回每次尝试的时间、HTTP 响应码/错误信息、最终状态（`queued` / `processing` / `delivered` / `dead` / `expired`），投递时间窗（`deliver_after` / `expires_at`），以及 `replay_count` / `last_replayed_at` 重放信息。
- **崩溃恢复**：投递状态全部在 SQLite（WAL 模式）中，重启后把卡在 `processing` 的事件重新排队；出站请求携带 `X-Event-Id` / `X-Idempotency-Key`，投递语义为 at-least-once。
- 内置一个实验用目标端点 `/dev/sink`，方便本地演示成功/失败/死信。

## 一条命令启动（Docker Compose）

```bash
docker compose up --build
```

启动后服务监听：

| 项目 | 地址 |
| --- | --- |
| API 根地址 | <http://localhost:8000> |
| 健康检查 | <http://localhost:8000/health> |
| 交互文档 (Swagger UI) | <http://localhost:8000/docs> |
| OpenAPI Schema | <http://localhost:8000/openapi.json> |

宿主机端口 `8000`，容器内端口 `8000`（在 `docker-compose.yml` 的 `ports` 中可改）。
数据（事件、投递尝试、死信与重放记录）持久化在命名卷 `webhook-data`（容器内
`/data/webhooks.db`）；老版本数据库启动时会自动补齐重放与时间窗相关列，无需手工迁移。

生产环境请覆盖默认密钥：

```bash
echo "WEBHOOK_SECRET=$(openssl rand -hex 32)" > .env
docker compose up --build
```

> 不想用 Compose 时：`docker build -t reliable-webhook . && docker run -p 8000:8000 -e WEBHOOK_SECRET=dev-secret-change-me -v webhook-data:/data reliable-webhook`

## 不用 Docker，本地直接运行

需要 Python 3.11+：

```bash
pip install -r requirements.txt
export WEBHOOK_SECRET=dev-secret-change-me
export WEBHOOK_DB_PATH=./data/webhooks.db
uvicorn app.main:app --host 0.0.0.0 --port 8000
```

## API

### 1. 创建事件 `POST /events`

请求头：

```
Content-Type: application/json
X-Webhook-Signature: sha256=<HMAC_SHA256(secret, 原始请求体) 的十六进制摘要>
```

请求体（JSON）：

| 字段 | 说明 |
| --- | --- |
| `idempotency_key` | 必填，调用方生成的唯一幂等键 |
| `target` | 必填，目标投递端点的完整 URL（http/https） |
| `payload` | 可选，任意 JSON，原样转发给目标端点 |
| `deliver_after` | 可选，RFC 3339 时间戳（须带时区偏移，如 `2026-09-17T13:00:00Z`）；该时刻之前不投递，默认为创建时刻 |
| `expires_at` | 可选，RFC 3339 时间戳，必须**晚于** `deliver_after`；到期时仍为 `queued` 的事件变为 `expired`，不再投递 |

- 签名缺失/错误 → `401`
- 时间窗无效（格式非法/缺少时区、`expires_at` 不晚于 `deliver_after`）→ `422`，不会写入任何数据
- 首次创建 → `201`
- 幂等键重复 → `200`，返回数据库中原事件（含其原始时间窗），带响应头 `Idempotent-Replay: true`，**不会重新投递**

### 2. 查询事件

- `GET /events/{event_id}` — 查询单个事件，含全部投递尝试
- `GET /events/key/{idempotency_key}` — 按幂等键查询
- `GET /events?status=dead&limit=50&offset=0` — 列表，可按状态过滤（`queued` / `processing` / `delivered` / `dead` / `expired`）

响应示例（4 次尝试后进入死信）：

```json
{
  "id": "308e5ff7…",
  "idempotency_key": "evt-0001",
  "target": "http://localhost:8000/dev/sink?fail_times=4",
  "payload": {"hello": "world"},
  "status": "dead",
  "attempts_count": 4,
  "created_at": "2026-09-17T12:27:13.160640+00:00",
  "deliver_after": "2026-09-17T12:27:13.160640+00:00",
  "expires_at": null,
  "delivered_at": null,
  "replay_count": 0,
  "last_replayed_at": null,
  "attempts": [
    {"attempt_no": 1, "status_code": 500, "error": null, "attempted_at": "2026-09-17T12:27:13.333930+00:00"},
    {"attempt_no": 2, "status_code": 500, "error": null, "attempted_at": "2026-09-17T12:27:14.498397+00:00"},
    {"attempt_no": 3, "status_code": 500, "error": null, "attempted_at": "2026-09-17T12:27:16.701202+00:00"},
    {"attempt_no": 4, "status_code": 500, "error": null, "attempted_at": "2026-09-17T12:27:20.934826+00:00"}
  ]
}
```

### 3. 重放死信事件 `POST /events/{event_id}/replay`

仅允许 `status=dead` 的事件。服务在同一个事务里把事件重新置为 `queued`
（`next_attempt_at=now`，首次尝试立即发生），并开启**新一轮完整**的 1s/2s/4s
退避——即新一轮再次享有 4 次尝试，而不是立即再次进入死信。

- 历史 `attempts` 全部保留，`attempt_no` 全局连续递增（如 4 次失败后重放，新尝试从 5 开始编号）
- `replay_count` 加一，`last_replayed_at` 记录本次重放时间
- 幂等键、事件 id、创建顺序（同端点有序性）均不变，出站签名与幂等头行为不变

响应与错误：

| 情况 | 状态码 |
| --- | --- |
| 重放成功，返回重放后的事件（`status=queued`） | `200` |
| 事件不存在 | `404` |
| 事件为 `queued` / `processing` / `delivered`（响应体说明当前状态） | `409` |

并发重复重放由条件更新（`UPDATE … WHERE status='dead'`）保证只有一个请求生效，
其余返回 `409`，**不会重复入队或重复投递**。

### 出站投递格式

服务向 `target` 发送 `POST`（`Content-Type: application/json`），请求体：

```json
{"id":"…","idempotency_key":"…","target":"…","payload":{…},"created_at":"…","attempt":1}
```

请求头：`X-Webhook-Signature`（用同一密钥对出站请求体签名）、`X-Event-Id`、
`X-Idempotency-Key`、`X-Delivery-Attempt`。目标端点返回任意 **2xx** 即视为成功。

## 快速试用（curl）

启动服务后另开一个终端：

```bash
BASE=http://localhost:8000
KEY="evt-$(date +%s)"
BODY="{\"idempotency_key\":\"$KEY\",\"target\":\"$BASE/dev/sink\",\"payload\":{\"msg\":\"hi\"}}"

# 用原始请求体计算签名
SIG="sha256=$(printf '%s' "$BODY" | openssl dgst -sha256 -hmac "${WEBHOOK_SECRET:-dev-secret-change-me}" | sed 's/^.* //')"

# 提交事件（/dev/sink 默认成功）
curl -s -X POST "$BASE/events" -H "X-Webhook-Signature: $SIG" -d "$BODY"

# 查询
curl -s "$BASE/events/key/$KEY"

# 演示失败重试 + 死信：让目标前 4 次都返回 500
BODY2="{\"idempotency_key\":\"${KEY}-fail\",\"target\":\"$BASE/dev/sink?fail_times=4\",\"payload\":{}}"
SIG2="sha256=$(printf '%s' "$BODY2" | openssl dgst -sha256 -hmac "${WEBHOOK_SECRET:-dev-secret-change-me}" | sed 's/^.* //')"
curl -s -X POST "$BASE/events" -H "X-Webhook-Signature: $SIG2" -d "$BODY2"
# 约 9 秒（1+2+4 退避）后 GET /events/key/... 可看到 4 条尝试与 status=dead

# 重放死信事件：重新入队并开启新一轮完整退避（仅 status=dead 允许）
DEAD_ID=$(curl -s "$BASE/events/key/${KEY}-fail" | python3 -c 'import json,sys; print(json.load(sys.stdin)["id"])')
curl -s -X POST "$BASE/events/$DEAD_ID/replay"

# 投递时间窗：60 秒后才允许投递，10 分钟后过期（到期仍未投递则变为 expired）
AFTER=$(date -u -d "+60 seconds" +%Y-%m-%dT%H:%M:%SZ 2>/dev/null || date -u -v+60S +%Y-%m-%dT%H:%M:%SZ)
EXPIRES=$(date -u -d "+10 minutes" +%Y-%m-%dT%H:%M:%SZ 2>/dev/null || date -u -v+10M +%Y-%m-%dT%H:%M:%SZ)
BODY3="{\"idempotency_key\":\"${KEY}-window\",\"target\":\"$BASE/dev/sink\",\"payload\":{},\"deliver_after\":\"$AFTER\",\"expires_at\":\"$EXPIRES\"}"
SIG3="sha256=$(printf '%s' "$BODY3" | openssl dgst -sha256 -hmac "${WEBHOOK_SECRET:-dev-secret-change-me}" | sed 's/^.* //')"
curl -s -X POST "$BASE/events" -H "X-Webhook-Signature: $SIG3" -d "$BODY3"
# 60 秒内查询可见 status=queued 且 attempts 为空；expires_at 不晚于 deliver_after 会得到 422
```

Python 生成签名：

```python
import hashlib, hmac
signature = "sha256=" + hmac.new(
    WEBHOOK_SECRET.encode(), raw_body_bytes, hashlib.sha256
).hexdigest()
```

## 投递语义与配置

| 环境变量 | 默认值 | 说明 |
| --- | --- | --- |
| `WEBHOOK_SECRET` | `dev-secret-change-me` | HMAC 签名密钥（入站校验 + 出站签名） |
| `WEBHOOK_DB_PATH` | `/data/webhooks.db` | SQLite 数据库路径 |
| `WORKER_POLL_INTERVAL` | `0.2` | 调度器扫描间隔（秒） |
| `WORKER_MAX_CONCURRENCY` | `8` | 同时进行投递的不同端点数 |
| `DELIVERY_TIMEOUT` | `10` | 出站 HTTP 超时（秒） |

重试计划固定为 **1s、2s、4s**（首次失败后），共 4 次尝试；如需调整可修改
`app/config.py` 中的 `RETRY_BACKOFF` / `MAX_ATTEMPTS`。

死信重放的实现要点：`events` 表上的 `round_base_attempts` 记录本轮退避的起点；
重放把它提升到当前 `attempts_count`，新一轮尝试的退避位置按轮内序号重新从 1s
开始，而 `attempt_no` 保持全局连续。启动时会自动为老数据库补齐 `replay_count` /
`last_replayed_at` / `round_base_attempts` 三列（`ALTER TABLE`，幂等），无需手工迁移。

投递时间窗的实现要点：`deliver_after` 在创建时写入 `next_attempt_at`（重试只会把
它往后推），因此调度器天然不会在到点之前领取事件；`expires_at` 另存一份 epoch 秒
（`expires_at_epoch`）用于比较。调度器每次领取前，先在**同一事务**内执行
`UPDATE events SET status='expired' WHERE status='queued' AND expires_at_epoch <= now`，
再按常规规则领取——过期事件因此永远不会产生投递尝试，并立即解除对同目标后续事件
的阻塞；`processing` 中的事件不在清扫范围内，投递不会被到期打断。老数据库启动时
自动补齐 `deliver_after` / `expires_at` / `expires_at_epoch` 三列，存量事件的
`deliver_after` 回填为 `created_at`（与旧行为一致：创建即可投递）、无过期时间。

有序性实现要点：调度器在一个事务中只挑选每个目标端点上「创建最早且退避已到期」
的事件并原子地置为 `processing`；更早的事件仍在 `queued`/`processing` 时，
后续事件不会被选中，直到前者 `delivered`、`dead` 或 `expired`。

## 运行测试

```bash
pip install -r requirements-dev.txt
python -m pytest tests/ -v
```

测试启动真实 uvicorn 端口，端到端覆盖：签名校验（缺失/错误/篡改）、成功投递与查询、
幂等不重复入队、1/2/4 秒退避后死信、失败后恢复、同端点严格有序（含前序失败阻塞后序直到死信）、
死信重放（完整新退避轮次、历史保留与连续编号、replay_count 累计、非死信 409/未知 404、并发重放仅生效一次）、
投递时间窗（默认窗口字段、非法窗口 422 且不落库、`deliver_after` 前不投递、未到时的前序事件仍阻塞同目标后续事件、
`queued` 到期转 `expired` 且无尝试并放行后续事件、`processing` 不受到期影响、幂等键重复返回原事件及其时间窗）。

## 项目结构

```
.
├── app/
│   ├── main.py        # FastAPI 路由：签名校验、事件接收、查询、死信重放、dev sink
│   ├── config.py      # 环境变量配置
│   ├── db.py          # SQLite 表结构、状态机、有序 claim/幂等查询
│   ├── delivery.py    # 出站 HTTP 投递与 HMAC 签名
│   └── worker.py      # 后台调度器（每端点有序、跨端点线程池并行）
├── tests/             # 端到端测试
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
└── README.md
```
