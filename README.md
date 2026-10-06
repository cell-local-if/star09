# Saga Orchestrator — 公开契约（baseline）

**任务编排与补偿**服务：工作流 = 一串步骤，每步可带一个补偿动作；某步失败时，**已完成步骤的补偿按逆序执行**。
本次基线只实现最小可用子集，后续任务在此契约之上继续建设。

## 运行

```bash
PYTHONPATH=src python3 -m saga.app --port 18897 --db saga.sqlite
PYTHONPATH=src python3 -m unittest discover -s tests -v
```

Python 3.12，**仅标准库**；`127.0.0.1`；实例持久化在 sqlite（WAL 无关，单文件）；`:memory:` 供测试。

## 概念

- **工作流定义**：`{"steps":[{"name":"reserve-stock","compensation":"release-stock"}, ...]}`（1–20 步）。
  每步可带可选 `retry` 对象，只允许 `{"maxAttempts": <整数 2..10>}`。
  每步可带可选 `await` 对象，只允许 `{"event": "<事件名>"}`，事件名为 ≤100 字符的非空字符串；
  含未知字段、缺 `event`、空值或非法类型 ⇒ `400 invalid_request`，已有同名定义保持不变。
- **实例状态**：`{"status":"running|completed|compensated", "step": <当前步骤名或 null>, "index": <int>,
  "attempt": <当前步骤的尝试序号，从 1 开始，进入新步骤时重置为 1>,
  "completed":[<已完成步骤名>], "compensated":[<将/已执行的补偿名，逆序>], "context": {...}, "failure": null|{...},
  "waitingFor": <当前步骤等待的事件名或 null>}`。
  到达带 `await` 的步骤时实例仍为 `running`，`step`/`index`/`attempt` 不变，`waitingFor` 为事件名；
  无等待时 `waitingFor` 为 `null`（终态亦为 `null`）。
- 状态转移是**纯函数**：同样的 `(状态, outcome)` 永远得到同样的下一个状态。

## 接口

### `GET /health`
`200 {"status":"ok"}`

### `GET /v1/workflows`
`200 {"workflows": ["order","provision", ...]}`（字典序；内置 `order`、`provision`）

### `PUT /v1/workflows/{name}`
请求体 = 工作流定义 → `200 {"workflow": name, "steps": [...]}`。
定义非法（step 非对象、`name` 空/超 100 字符、未知字段、步数越界）⇒ `400 invalid_request`。
步骤的 `retry` 只允许 `{"maxAttempts": n}`（整数，2–10）；`retry` 缺失 `maxAttempts`、含未知字段、
值为布尔/非整数、小于 2 或大于 10 ⇒ `400 invalid_request`，已有定义保持不变。

### `POST /v1/workflows/{name}/instances`
请求体：`{"context": {...}}`（可省）→ **`201`** `{"id": <uuid>, "workflow": name, "state": {...}}`。

### `POST /v1/instances/{id}/events`
请求体：`{"outcome": "succeeded"|"failed", "detail": <任意 JSON>, "eventId": "<可选>"}` →
`200 {"id":..., "workflow":..., "state": {...}}`。
- `succeeded`：推进到下一步（`attempt` 重置为 1，`failure` 清除）；已是最后一步 ⇒ `status="completed"`。
- `failed`：
  - 当前步骤配置了 `retry` 且 `attempt < maxAttempts`：实例保持 `running`，`step`/`index` 不变，
    `attempt` 加一，`failure` 记录本步 `step` 与 `detail`，`completed`/`compensated` 不变。
  - 否则（无 `retry`，或 `attempt` 已等于 `maxAttempts`）：`status="compensated"`，
    `compensated` = 本步及之前所有**带补偿**的步骤名**逆序**。
- 实例已终态（completed/compensated）再发事件 ⇒ **`409 invalid_transition`**；`outcome` 非法 ⇒ `400`。
- 当前步骤带 `await`（`waitingFor` 非空）时，`/events` 一律 ⇒ **`409 invalid_transition`**；
  收到匹配信号、步骤推进之后，才能继续对新的当前步骤提交 `succeeded`/`failed`。

### `POST /v1/instances/{id}/signals`
请求体：`{"event": "<事件名>", "detail": <任意 JSON>, "eventId": "<可选>"}` →
`200 {"id":..., "workflow":..., "state": {...}}`。
- 仅当实例 `running` 且当前步骤 `waitingFor` 与 `event` 相同才接受：当前步骤按**成功结果**完成
  （`completed` 加入步骤名，`failure` 清除，`attempt` 重置为 1）并进入下一步；下一步仍带 `await`
  则 `waitingFor` 更新为新事件名；若已是最后一步则 `status="completed"`，`step` 与 `waitingFor` 均为 `null`。
- 实例不存在 ⇒ `404 not_found`；实例终态、当前步骤无 `await`（`waitingFor` 为 `null`）或事件名不匹配
  ⇒ **`409 invalid_transition`**；请求体不是对象、含未知字段、缺 `event` 或 `event` 为非法类型
  ⇒ **`400 invalid_request`**（`event` 与 `eventId` 同样必须是 ≤100 字符的非空字符串）。
  这些拒绝都不改变状态、不写幂等账本。

#### 信号幂等（可选 `eventId`）
- 规则与 `/events` 完全一致：`eventId` 必须是非空字符串且 ≤100 字符，仅在单实例范围内唯一；
  首次合法信号原子推进并把归一化请求（`eventId`/`event`/`detail`；省略 `detail` 与显式 `null` 等值）
  与该次**完整 JSON 响应**写入信号账本（SQLite 表 `instance_signals`，与实例状态同事务）。
- 相同 `(实例, eventId)` 再次提交：`event` 与 `detail` 的 JSON 值相同 ⇒ 返回首次 200 响应原文、
  不改状态；不同 ⇒ **`409 invalid_transition`**。并发重复只有一次首次处理，其余得到首次响应。
- 信号账本与事件账本相互独立，同一 `eventId` 字符串可分别用于一个事件和一个信号。

#### 事件幂等（可选 `eventId`）
- 不带 `eventId`：行为不变，每次调用都重新进入状态机（终态实例仍返回 `409`）。
- `eventId` 必须是**非空字符串且 ≤100 字符**；为 `null`、空串、非字符串或超长 ⇒ `400 invalid_request`。
- `eventId` 仅在**单个实例范围内**唯一；不同实例可复用相同标识。
- 首次合法事件：原子地推进状态，并把事件标识、归一化请求（`eventId`/`outcome`/`detail`；
  省略 `detail` 与显式 `null` 视为相同值）和该次处理的**完整 JSON 响应**写入实例事件账本
  （SQLite 表 `instance_events`，与实例状态同库同事务；服务重开同一文件后仍可命中）。
- 相同 `(实例, eventId)` 再次提交：
  - `outcome` 相同且 `detail` 的 JSON 值相同 ⇒ 返回**首次处理的 200 响应原文**（含当时的
    `state`），不改变当前状态，不重复累计 `completed`/`compensated`/`failure`（终态后回放仍返回 200）。
  - `outcome` 或 `detail` 不同 ⇒ **`409 invalid_transition`**，状态不变。
- 非法 `outcome`、终态推进、不存在实例的请求**不写账本**。
- 并发提交同一实例的事件等价于某个确定的串行顺序；同一 `eventId` 并发时一个请求完成首次处理，
  其余请求得到首次响应。
- 新表均以 `CREATE TABLE IF NOT EXISTS` 建立，旧 SQLite 文件直接可读；旧实例带上 `eventId`
  后照常读取与推进。不含 `attempt` 字段的旧实例状态直接可读，首次推进时按 `attempt=1` 补齐；
  不含 `waitingFor` 字段的旧实例按 `null` 读取（即不处于等待状态），可继续接受原有 `/events`。
  等待状态随实例状态持久化、信号幂等记录随 `instance_signals` 持久化，重开服务后等待与回放结果一致。

### `GET /v1/instances/{id}`
`200 {"id","workflow","state"}`；未知 id ⇒ `404 not_found`。

## 错误语义

```json
{"error": {"code": "invalid_request|not_found|invalid_transition|internal_error", "message": "<可读说明>"}}
```

## 未实现（后续任务候选，非固定题单）

超时与死信、并发与抢占、编排版本迁移与在飞实例、分区与顺序保证、
持久化恢复与重放、限流与背压、可视化查询与审计回放、失败注入测试。
