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
  每步可带可选 `await` 对象，只允许 `{"event": "<事件名>", "timeoutMs": <可选整数 1..86400000>}`，
  事件名为 ≤100 字符的非空字符串；`timeoutMs` 为等待超时毫秒数（布尔、小数、非整数、越界均非法）。
  含未知字段、缺 `event`、空值或非法类型 ⇒ `400 invalid_request`，已有同名定义保持不变；
  不带 `timeoutMs` 的 `await` 保持原有语义（无限期等待）。
- **实例状态**：`{"status":"running|completed|compensated|dead_lettered", "step": <当前步骤名或 null>, "index": <int>,
  "attempt": <当前步骤的尝试序号，从 1 开始，进入新步骤时重置为 1>,
  "completed":[<已完成步骤名>], "compensated":[<将/已执行的补偿名，逆序>], "context": {...}, "failure": null|{...},
  "waitingFor": <当前步骤等待的事件名或 null>, "deadlineAt": <等待截止时刻，Unix epoch 毫秒整数或 null>}`。
  到达带 `await` 的步骤时实例仍为 `running`，`step`/`index`/`attempt` 不变，`waitingFor` 为事件名；
  该 `await` 带 `timeoutMs` 时 `deadlineAt` 为进入等待时刻 + `timeoutMs`，否则为 `null`；
  无等待时 `waitingFor` 与 `deadlineAt` 均为 `null`（终态亦为 `null`）。
  旧状态缺少 `waitingFor`/`deadlineAt` 字段时按 `null` 读取。
- 状态转移是**纯函数**：同样的 `(状态, outcome, 当前时刻)` 永远得到同样的下一个状态。

## 接口

### `GET /health`
`200 {"status":"ok"}`

### `GET /v1/workflows`
`200 {"workflows": ["order","provision", ...]}`（字典序；内置 `order`、`provision`）

### `PUT /v1/workflows/{name}`
请求体 = 工作流定义 → `200 {"workflow": name, "version": <int>, "steps": [...]}`。
定义非法（step 非对象、`name` 空/超 100 字符、未知字段、步数越界）⇒ `400 invalid_request`。
步骤的 `retry` 只允许 `{"maxAttempts": n}`（整数，2–10）；`retry` 缺失 `maxAttempts`、含未知字段、
值为布尔/非整数、小于 2 或大于 10 ⇒ `400 invalid_request`，已有定义保持不变。

**版本**：每次合法提交生成一个**不可变版本**（SQLite 表 `workflow_versions`，`(name, version)` 主键，
只插不改）；内置 `order`、`provision` 从版本 1 开始，后续 PUT 依次得到 2、3…；
非法提交不产生新版本。已生成的版本永不改写，重开同一文件后自定义定义与版本序号仍然存在。

### `GET /v1/workflows/{name}`
`200 {"workflow": name, "version": <最新版本号>, "steps": [...]}`；未知工作流 ⇒ `404 not_found`。

### `POST /v1/workflows/{name}/instances`
请求体：`{"context": {...}, "version": <可选正整数>}`（均可省）→
**201** `{"id": <uuid>, "workflow": name, "workflowVersion": <int>, "state": {...}}`。
省略 `version` 启动最新版；指定正整数则把实例**固定**到该版本——之后同名定义再 PUT 新版本
不影响该实例的推进语义。`version` 非正整数（含布尔、小数、字符串）⇒ `400 invalid_request`；
版本不存在 ⇒ `404 not_found`；未知工作流名 ⇒ `400 invalid_request`（同基线）。
实例的 `workflowVersion` 随实例行持久化（`instances.version` 列），重开后保持一致。

### `POST /v1/instances/{id}/events`
请求体：`{"outcome": "succeeded"|"failed"|"timed_out", "detail": <任意 JSON>, "eventId": "<可选>"}` →
`200 {"id":..., "workflow":..., "workflowVersion": <推进所用版本>, "state": {...}}`。
- `succeeded`：推进到下一步（`attempt` 重置为 1，`failure` 清除）；已是最后一步 ⇒ `status="completed"`。
- `failed`：
  - 当前步骤配置了 `retry` 且 `attempt < maxAttempts`：实例保持 `running`，`step`/`index` 不变，
    `attempt` 加一，`failure` 记录本步 `step` 与 `detail`，`completed`/`compensated` 不变。
  - 否则（无 `retry`，或 `attempt` 已等于 `maxAttempts`）：`status="compensated"`，
    `compensated` = 本步及之前所有**带补偿**的步骤名**逆序**。
- `timed_out`：等待超时死信。仅当实例 `running`、当前步骤正在等待（`waitingFor` 非空）、
  `deadlineAt` 非空且已到期（当前时刻 ≥ `deadlineAt`）才接受：
  - `status="dead_lettered"`（终态），`step`/`index` 保留超时位置，`completed`/`compensated` 不变；
  - `failure = {"step": <超时步骤名>, "reason": "timeout", "detail": <请求 detail>}`；
  - `waitingFor` 与 `deadlineAt` 清为 `null`。
  未到期限、当前步骤非等待、等待无 `timeoutMs`（`deadlineAt` 为 `null`）、实例已终态
  ⇒ **`409 invalid_transition`**；实例不存在 ⇒ `404 not_found`。
  期限之前到达的匹配信号仍按原成功路径推进并清除 `deadlineAt`（见 `/signals`）。
- 实例已终态（completed/compensated/dead_lettered）再发事件 ⇒ **`409 invalid_transition`**；
  `outcome` 非法 ⇒ `400 invalid_request`。
- 当前步骤带 `await`（`waitingFor` 非空）时，`succeeded`/`failed` 一律 ⇒ **`409 invalid_transition`**；
  收到匹配信号、步骤推进之后，才能继续对新的当前步骤提交 `succeeded`/`failed`。

### `POST /v1/instances/{id}/signals`
请求体：`{"event": "<事件名>", "detail": <任意 JSON>, "eventId": "<可选>"}` →
`200 {"id":..., "workflow":..., "workflowVersion":..., "state": {...}}`。
- 仅当实例 `running` 且当前步骤 `waitingFor` 与 `event` 相同才接受：当前步骤按**成功结果**完成
  （`completed` 加入步骤名，`failure` 清除，`attempt` 重置为 1）并进入下一步；下一步仍带 `await`
  则 `waitingFor` 更新为新事件名（带 `timeoutMs` 时同时设置新的 `deadlineAt`）；若已是最后一步则
  `status="completed"`，`step`、`waitingFor` 与 `deadlineAt` 均为 `null`。
  期限之前到达的匹配信号始终走此成功路径，不受 `timeoutMs` 影响。
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
  - `outcome` 或 `detail` 不同 ⇒ **`409 invalid_transition`**，状态不变，不写账本。
- `timed_out` 与 `succeeded`/`failed` 共用同一事件账本与同一套幂等规则：相同载荷回放首次
  响应原文（含 `dead_lettered` 状态与 `failure`），不同 `outcome` 或 `detail` ⇒ `409`。
- 非法 `outcome`、终态推进、未到期限/非等待/无超时的 `timed_out`、不存在实例的请求**不写账本**。
- 并发提交同一实例的事件等价于某个确定的串行顺序；同一 `eventId` 并发时一个请求完成首次处理，
  其余请求得到首次响应。
- 新表均以 `CREATE TABLE IF NOT EXISTS` 建立，旧 SQLite 文件直接可读；旧实例带上 `eventId`
  后照常读取与推进。不含 `attempt` 字段的旧实例状态直接可读，首次推进时按 `attempt=1` 补齐；
  不含 `waitingFor` 字段的旧实例按 `null` 读取（即不处于等待状态），可继续接受原有 `/events`；
  不含 `deadlineAt` 字段的旧实例同样按 `null` 读取（等待无超时，`timed_out` 对其返回 `409`）。
  等待状态（含 `deadlineAt`）随实例状态持久化、信号幂等记录随 `instance_signals` 持久化，
  重开服务后等待、死信状态、`failure` 与回放结果一致。

### `POST /v1/instances/{id}/migrate`
在飞迁移：把 `running` 实例重新固定到**同名工作流的另一个现存版本**。
请求体：`{"version": <正整数>}` → `200 {"id","workflow","workflowVersion","state"}`。
- 兼容性：目标版本的步骤总数与有序步骤名须与当前版本一致，且当前 `index` 对应步骤及之前
  所有步骤的定义（name/compensation/retry/await）完全一致；只有**未进入步骤**的
  compensation/retry/await 允许调整。
- 迁移成功只改变版本归属：`status`/`step`/`index`/`attempt`/`completed`/`compensated`/
  `context`/`failure`/`waitingFor`/`deadlineAt` 全部原样保留，后续推进按目标版本定义执行；
  不写幂等账本、不追加审计记录。
- `version` 缺失或非正整数 ⇒ `400 invalid_request`；实例不存在或目标版本不存在 ⇒
  `404 not_found`；实例已终态、步骤序列不兼容、已进入步骤定义不同 ⇒ `409 invalid_transition`。
  所有拒绝都不改变状态、账本与审计。
- 未固定版本的旧实例（`workflowVersion` 为 `null`）按"当前跟随最新版"衡量兼容性，
  迁移成功的同时完成版本固定。
- 历史响应保留事件发生时的版本：迁移不改写幂等账本与审计记录中已存的 `workflowVersion`。

### `GET /v1/instances/{id}`
`200 {"id","workflow","workflowVersion","state"}`；未知 id ⇒ `404 not_found`。
旧 SQLite 文件（无 `version` 列）中的实例 `workflowVersion` 为 `null`；
该实例首次成功推进时按当时最新版补记并固定，之后的查询与响应都携带该版本。

### `GET /v1/instances/{id}/audit`
实例级只读审计查询（无请求体）→ `200 {"id","workflow","workflowVersion","state","history":[...]}`；未知 id ⇒ `404 not_found`。
- `state` 为查询时的当前状态（与 `GET /v1/instances/{id}` 一致）；`history` 按 `seq` 从 1 开始严格递增，
  只收录**真正推进状态机**的 `/events` 与 `/signals` 调用。
- 每条记录：`{"seq","kind","eventId","request","response"}`；`kind` 为 `event`（/events）或
  `signal`（/signals），同一 `eventId` 字符串被事件与信号复用时通过 `kind` 区分。
- `request` 保存归一化输入（`eventId`/`outcome`/`detail` 或 `eventId`/`event`/`detail`）；
  未提交 `eventId` 时记录中的 `eventId` 为 `null`；省略 `detail` 与显式 `null` 都记为 `null`；
  被接受的 `timed_out` 以 `kind="event"`、`request.outcome="timed_out"` 记录，`seq` 连续递增。
- `response` 保存该次处理返回的完整 JSON 响应，无需重新执行状态机即可逐条核对当时结果。
- 幂等回放（相同 `eventId` 相同载荷）仍只返回首次响应且只占一条审计记录，即使实例后来已终态；
  非法请求、相同 `eventId` 不同载荷、终态推进、等待期间提交事件、信号名不匹配等被拒绝的调用
  均不追加记录。不同 `eventId` 的并发接受结果按某个确定串行顺序获得连续 `seq`。
- 审计记录与状态更新、幂等账本在**同一事务**提交（SQLite 表 `instance_audit`），
  服务重开同一文件后 `history`、`seq` 与当前 `state` 保持一致。
- 新表以 `CREATE TABLE IF NOT EXISTS` 建立，旧 SQLite 文件直接可读且既有读取与回放行为不变；
  升级前的调用**不伪造补录**，旧实例从升级后的下一次成功推进开始积累历史。

## 错误语义

```json
{"error": {"code": "invalid_request|not_found|invalid_transition|internal_error", "message": "<可读说明>"}}
```

## 未实现（后续任务候选，非固定题单）

并发与抢占、分区与顺序保证、
持久化恢复与重放、限流与背压、可视化查询与审计回放、失败注入测试。
