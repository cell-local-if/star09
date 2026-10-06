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
  每步可带可选 `retry` 对象，只允许 `{"maxAttempts": <整数 2..10>}`；
  可带可选 `await` 对象，只允许 `{"event": "<事件名>"}`（非空字符串，≤100 字符）。
- **实例状态**：`{"status":"running|completed|compensated", "step": <当前步骤名或 null>, "index": <int>,
  "attempt": <当前步骤的尝试序号，从 1 开始，进入新步骤时重置为 1>,
  "completed":[<已完成步骤名>], "compensated":[<将/已执行的补偿名，逆序>], "context": {...},
  "failure": null|{...}, "waitingFor": <正在等待的事件名或 null>}`。
  旧实例缺少 `waitingFor` 字段时按 `null` 读取。
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
步骤的 `await` 只允许 `{"event": "<事件名>"}`（非空字符串，≤100 字符）；`await` 为 null/非对象、
缺失 `event`、含未知字段、事件名为空/非字符串/超长 ⇒ `400 invalid_request`，已有定义保持不变。

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
- 实例处于等待（`waitingFor` 非 null）时，本接口一律返回 **`409 invalid_transition`**；
  收到匹配信号（见下）后才可继续提交 `succeeded`/`failed`。

### `POST /v1/instances/{id}/signals`
请求体：`{"event": "<事件名>", "detail": <任意 JSON>, "eventId": "<可选>"}` →
`200 {"id":..., "workflow":..., "state": {...}}`。
- 当前步骤带 `await` 时实例停留在该步（`running`，`step`/`index`/`attempt` 不变），
  `waitingFor` 为事件名；`event` 与 `waitingFor` 匹配时按**成功结果**完成当前步骤：
  `completed` 加入步骤名、`failure` 清除、`attempt` 重置为 1 并进入下一步；
  下一步仍带 `await` 则 `waitingFor` 更新为新事件名，已是最后一步则 `status="completed"`
  且 `step`/`waitingFor` 均为 `null`。
- 请求体非法（缺 `event`、未知字段、`event` 空/非字符串/超长）或 `eventId` 非法 ⇒ `400 invalid_request`；
  实例不存在 ⇒ `404 not_found`；当前无等待或事件名不匹配 ⇒ `409 invalid_transition`。
  这些拒绝不改变状态，也不写幂等账本。
- `eventId` 的校验、实例范围幂等与完整响应回放规则与事件接口完全一致（同一张
  `instance_events` 账本）：同标识同 `event`/`detail` 返回首次 200 响应原文且不改状态，
  不同则 `409 invalid_transition`；并发重复只有一次首次处理。

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
- 新表以 `CREATE TABLE IF NOT EXISTS` 建立，旧 SQLite 文件直接可读；旧实例带上 `eventId`
  后照常读取与推进。不含 `attempt` 字段的旧实例状态直接可读，首次推进时按 `attempt=1` 补齐。

### `GET /v1/instances/{id}`
`200 {"id","workflow","state"}`；未知 id ⇒ `404 not_found`。

## 错误语义

```json
{"error": {"code": "invalid_request|not_found|invalid_transition|internal_error", "message": "<可读说明>"}}
```

## 未实现（后续任务候选，非固定题单）

超时与死信、并发与抢占、编排版本迁移与在飞实例、分区与顺序保证、
持久化恢复与重放、限流与背压、可视化查询与审计回放、失败注入测试。
