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
- **定义版本**：每次合法 `PUT` 为同名工作流追加一个**不可变版本**（整数，从 1 开始单调递增；
  内置 `order`、`provision` 预置为版本 1）。版本持久化在 SQLite 表 `workflow_versions`，
  从不更新或删除，重开同一文件后自定义定义与全部历史版本仍在。
  每个实例启动时**固定**一个版本（省略则为当时最新版），后续 `PUT` 不影响在飞实例；
  实例推进、重试、补偿与等待判定一律使用固定版本的定义。
- **实例状态**：`{"status":"running|completed|compensated|dead_lettered", "step": <当前步骤名或 null>, "index": <int>,
  "attempt": <当前步骤的尝试序号，从 1 开始，进入新步骤时重置为 1>,
  "completed":[<已完成步骤名>], "compensated":[<将/已执行的补偿名，逆序>], "context": {...}, "failure": null|{...},
  "waitingFor": <当前步骤等待的事件名或 null>, "deadlineAt": <等待截止时刻，Unix epoch 毫秒整数或 null>}`。
  到达带 `await` 的步骤时实例仍为 `running`，`step`/`index`/`attempt` 不变，`waitingFor` 为事件名；
  该 `await` 带 `timeoutMs` 时 `deadlineAt` 为进入等待时刻 + `timeoutMs`，否则为 `null`；
  无等待时 `waitingFor` 与 `deadlineAt` 均为 `null`（终态亦为 `null`）。
  旧状态缺少 `waitingFor`/`deadlineAt` 字段时按 `null` 读取。
- **实例版本（乐观并发）**：每个实例从版本 **1** 开始；版本只在**真正改变实例**时加 1
  （被接受的事件/信号推进状态机后 +1，成功迁移 +1，成功恢复 +1；迁移仍逐字段保留迁移前 `state`，
  恢复只复位 `status`/`failure`/`attempt` 与等待字段，但版本照常 +1）。
  非法事件、终态继续推进、等待期间提交普通结果、信号名不匹配、未到期的 `timed_out`、不兼容迁移、
  对非 `dead_lettered` 实例（含已恢复实例）的恢复
  均**不加版本**，状态、版本、幂等账本、审计都不变。
  版本随实例行持久化（`instances.instance_version` 列，旧文件自动 `ALTER TABLE` 补齐）；
  旧实例（无该列）按版本 **1** 读取，下一次真正变更后变为 **2**——不伪造历史变更次数。
  实例版本与“定义版本”（`workflowVersion`）相互独立。
- **持久化分区顺序（可选）**：`/events` 与 `/signals` 可携带 `partitionKey`（1–100 字符非空字符串）与
  `sequence`（正整数），且必须同时出现并带非空 `eventId` 才启用。分区按 **(工作流名, partitionKey)**
  隔离、跨实例共享，事件与信号共用从 1 连续递增的游标（表 `partition_cursors`）；首个序号必须为 1，
  之后严格 +1，跳号/旧号返回 `409 invalid_transition` 且不缓冲。详见接口节“持久化分区顺序”。
- 状态转移是**纯函数**：同样的 `(状态, outcome, 当前时刻)` 永远得到同样的下一个状态。

## 接口

### `GET /health`
`200 {"status":"ok"}`

### `GET /v1/workflows`
`200 {"workflows": ["order","provision", ...]}`（字典序；内置 `order`、`provision`）

### `PUT /v1/workflows/{name}`
请求体 = 工作流定义 → `200 {"workflow": name, "version": <新版本号>, "steps": [...]}`。
每次合法提交追加一个不可变版本（首个版本为 1，之后 2、3、…），已有版本永不被改写。
定义非法（step 非对象、`name` 空/超 100 字符、未知字段、步数越界）⇒ `400 invalid_request`，
已有定义与版本序列保持不变。
步骤的 `retry` 只允许 `{"maxAttempts": n}`（整数，2–10）；`retry` 缺失 `maxAttempts`、含未知字段、
值为布尔/非整数、小于 2 或大于 10 ⇒ `400 invalid_request`，已有定义保持不变。

### `GET /v1/workflows/{name}`
`200 {"workflow": name, "version": <最新版本号>, "steps": [...]}`；未知工作流 ⇒ `404 not_found`。

### `POST /v1/workflows/{name}/instances`
请求体：`{"context": {...}, "version": <可选正整数>}`（均可省）→ **`201`**
`{"id": <uuid>, "workflow": name, "workflowVersion": <固定的版本号>, "state": {...}}`，
并带响应头 **`ETag: "1"`**（每个实例从版本 1 开始）。
省略 `version` 启动当时最新版；指定正整数则固定到该现存版本。
`version` 非正整数（含布尔、小数、字符串、0、负数）⇒ `400 invalid_request`；
版本不存在 ⇒ `404 not_found`；未知工作流（未指定版本）⇒ `400 invalid_request`。

### `POST /v1/instances/{id}/events`
请求体：`{"outcome": "succeeded"|"failed"|"timed_out", "detail": <任意 JSON>, "eventId": "<可选>",
"partitionKey": "<可选>", "sequence": <可选正整数>}` →
`200 {"id":..., "workflow":..., "workflowVersion":..., "state": {...}}`（有序成功调用额外含
`"ordering": {"partitionKey": ..., "sequence": ...}`，见“持久化分区顺序”），成功响应（含幂等回放）带
**`ETag: "<当前实例版本>"`**。请求可带可选 **`If-Match: "<版本>"`** 前置条件（见下节“实例版本与 If-Match”）。
- `succeeded`：推进到下一步（`attempt` 重置为 1，`failure` 清除）；已是最后一步 ⇒ `status="completed"`。
- `failed`：
  - 当前步骤配置了 `retry` 且 `attempt < maxAttempts`：实例保持 `running`，`step`/`index` 不变，
    `attempt` 加一，`failure` 记录本步 `step` 与 `detail`，`completed`/`compensated` 不变。
  - 否则（无 `retry`，或 `attempt` 已等于 `maxAttempts`）：`status="compensated"`，
    `compensated` = 本步及之前所有**带补偿**的步骤名**逆序**。
- `timed_out`：等待超时死信。仅当实例 `running`、当前步骤正在等待（`waitingFor` 非空）、
  `deadlineAt` 非空且已到期（当前时刻 ≥ `deadlineAt`）才接受：
  - `status="dead_lettered"`（对普通事件、外部信号与版本迁移为终态；唯一例外是显式
  `POST /v1/instances/{id}/recover`，见该节），`step`/`index` 保留超时位置，`completed`/`compensated` 不变；
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
请求体：`{"event": "<事件名>", "detail": <任意 JSON>, "eventId": "<可选>",
"partitionKey": "<可选>", "sequence": <可选正整数>}` →
`200 {"id":..., "workflow":..., "workflowVersion":..., "state": {...}}`（有序成功调用额外含
`"ordering": {"partitionKey": ..., "sequence": ...}`，见“持久化分区顺序”），成功响应（含幂等回放）带
**`ETag: "<当前实例版本>"`**；请求可带可选 **`If-Match`**（规则与 `/events` 完全一致）。
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
- 实例的固定版本随实例行持久化（`instances.version` 列，旧文件自动 `ALTER TABLE` 补齐、
  旧实例为 `NULL`）；工作流定义随 `workflow_versions` 持久化，重开同一文件后自定义实例
  仍能拿到启动时的定义，版本归属与回放响应（含其中的 `workflowVersion`）保持不变。

#### 持久化分区顺序（可选 `partitionKey`/`sequence`）
`/events` 与 `/signals` 都可在事件幂等之上再启用**持久化分区顺序**。规则对两个入口完全一致：
- **启用条件**：仅当请求**同时**携带 `partitionKey` 与 `sequence`，且携带非空 `eventId` 时启用；
  不带任一字段、只给一个字段、或两个字段都给了但没有 `eventId`，均不启用。
  - `partitionKey` 必须是 **1 到 100 字符的非空字符串**；
  - `sequence` 必须是**正整数**（JSON 整数且 ≥ 1；布尔、小数、字符串、`0`、负数、`null` 均非法）；
  - 上述任何非法形态（含只给一个字段或缺 `eventId`）⇒ **`400 invalid_request`**，不产生任何写入。
- **分区与游标**：分区按 **(工作流名, partitionKey)** 隔离，**跨实例**共享；同一分区内
  `/events` 与 `/signals` **共用一个从 1 连续递增的游标**（SQLite 表 `partition_cursors`，
  每个分区一行）。不同工作流同名 partitionKey、或同一工作流不同 partitionKey 都是互不影响的独立分区。
- **序号规则**：分区的首次有序调用 `sequence` 只能为 **1**；此后被接受的有序调用按游标 +1 连续占用。
  - 跳号（游标为 n 却提交 n+2 或更大）、旧号（≤ n）⇒ **`409 invalid_transition`**；
    **不缓冲、不重排**——乱序消息直接拒绝，客户端须以正确序号重发。
  - 序号正确但状态机本身拒绝（终态推进、等待中提交事件、信号名不匹配、未到期 `timed_out` 等）
    同样 ⇒ `409 invalid_transition`，且**不占用序号**：游标不变，下一次调用仍用该序号（可发给同分区另一实例）。
  - 同分区并发时，**最多一个**调用取得当前序号；其余（即使 `eventId` 不同）得到 `409 invalid_transition`，
    且不写账本、不追加审计。
- **校验顺序**：有序调用在同一事务内依次判定 **① eventId 回放 → ② If-Match → ③ 序号 → ④ 状态机**：
  - **回放优先**：同入口（同 `/events` 或同 `/signals`）内相同 `(实例, eventId)` 且归一化请求相同
    （含 `partitionKey`/`sequence`、以及 `outcome` 或 `event`、`detail` 全部一致）的重试，
    返回**首次 200 响应原文**（含首次的 `ordering` 与历史 `state`），不推进状态、不占序号、不追加审计，
    即使携带的 `If-Match` 已过期或序号已是旧号。
  - 相同 `eventId` 但载荷不同（`partitionKey`/`sequence`/`outcome`/`event`/`detail` 任一不同）
    ⇒ **`409 invalid_transition`**（先于 If-Match 与序号判定）。
  - 新 `eventId` 的 `If-Match` 失配 ⇒ `409 invalid_transition`（先于序号判定）；
    If-Match 通过但序号失配 ⇒ `409 invalid_transition`。
  - 所有拒绝都不改状态、实例版本、账本、审计与游标。
- **成功语义**：序号正确且状态机接受的调用，在**同一事务**更新状态机、幂等账本、审计与分区游标，
  实例版本照常 +1；成功响应新增 **`"ordering": {"partitionKey": <字符串>, "sequence": <整数>}`**。
  回放响应保持首次响应（含首次 `ordering`）。
- **不带排序字段的请求**完全保持原有返回、幂等、ETag、重试、等待、超时、迁移、补偿与审计行为，
  归一化请求与响应均不含排序字段，**不创建任何分区记录**。
- **审计**：`GET /v1/instances/{id}/audit` 中有序调用的 `request` 在原有字段之外保存
  `partitionKey` 与 `sequence`，`response` 保存含 `ordering` 的完整首次响应；被拒绝的调用不记录，
  升级前的旧记录不补顺序字段。
- **持久化与旧库**：游标随同一 SQLite 文件持久化，重开后游标、账本与审计一致，分区从游标下一号继续。
  旧文件以 `CREATE TABLE IF NOT EXISTS` 补建游标表（初始为空，不伪造历史），旧记录不补顺序，
  新分区一律从 1 开始；升级前写入的无排序字段账本行，升级后仍按原样回放，不会被误判为载荷冲突。
- 未知实例的有序调用仍返回 **`404 not_found`**（不创建分区行）；非法推进仍返回 `409 invalid_transition`。

### `GET /v1/instances`
实例列表只读查询（无请求体，纯读取、不产生任何写入），用于按工作流、状态与稳定分页观察在飞任务、
补偿结果与死信分布 → `200 {"instances": [...], "nextCursor": <本页最后一项 id 或 null>}`。
- **查询参数**（全部可选，省略即不过滤或使用默认值；参数不得重复、不得出现未知参数）：
  - `workflow`：按工作流名**精确**过滤，1–100 字符的非空字符串；
  - `status`：只允许 `running`、`completed`、`compensated`、`dead_lettered`；
  - `afterId`：实例 id 的**字节序边界**（1–200 字符的非空字符串），结果从边界之后（不含边界）开始；
    它只是排序位置，**不要求对应真实实例**；
  - `limit`：1–100 的**无前导零**十进制整数字符串，默认 `50`。
- **结果**：实例按 id 升序（字节序）返回，筛选、排序与分页稳定。列表项为
  `{"id", "workflow", "workflowVersion", "state"}`：`workflowVersion` 对版本化之前持久化的旧实例为
  `null`；`state` 与 `GET /v1/instances/{id}` 当前公开的结构一致（含旧字段回填规则）。
- **分页**：还有后续匹配项时 `nextCursor` 取本页最后一项的 id（作为下一页的 `afterId`），否则为
  `null`；合法但无匹配实例时返回 `200`、空列表与 `null`。
- **参数非法**：`workflow` 为空或超 100 字符、`status` 不在允许集合、`afterId` 为空或超 200 字符、
  `limit` 不是无前导零十进制整数或不在 1–100、参数重复或含未知参数
  ⇒ `400 invalid_request`，且不产生任何写入。

### `GET /v1/instances/{id}`
`200 {"id","workflow","workflowVersion","state"}`，并带响应头 **`ETag: "<实例版本>"`**；未知 id ⇒ `404 not_found`。
`workflowVersion` 为实例固定的定义版本；版本化之前持久化的旧实例（SQLite 中无版本字段）返回
`null`，并在首次成功推进时补记当时最新版（之后照常固定）。
实例版本与定义版本无关：新实例及旧实例都按版本 `1` 读取。

### `POST /v1/instances/{id}/migrate`
请求体：`{"version": <正整数>}`（可带可选 `If-Match`）→ `200 {"id","workflow","workflowVersion","state"}`，
成功响应带 **`ETag: "<迁移后的实例版本>"`**（迁移是真正的实例变更，版本 +1）。
把 **running** 实例重新固定到同名工作流的另一个**现存版本**：
- 目标版本的步骤总数与有序步骤名必须与当前版本一致；
- 当前 `index` 对应步骤及之前所有步骤的定义（`name`/`compensation`/`retry`/`await`）必须完全一致，
  只有**未进入**步骤的 `compensation`、`retry`、`await` 允许不同；
- 迁移不改写状态：`status`、`step`、`index`、`attempt`、`completed`、`compensated`、`context`、
  `failure`、`waitingFor`、`deadlineAt` 全部保留，后续推进改用目标版本定义；
- 迁移不写幂等账本、不追加审计记录；账本与审计中已存的历史响应保留事件发生时的版本，永不改写。
  但迁移本身是实例变更：实例版本 +1（`state` 字段仍逐字段保留）。
- `version` 缺失或非正整数 ⇒ `400 invalid_request`；实例不存在或版本不存在 ⇒ `404 not_found`；
  实例已终态、实例尚无版本记录、步骤序列不兼容或已进入步骤定义不同
  ⇒ `409 invalid_transition`；带了与当前实例版本不相等的 `If-Match` 同样 ⇒ `409 invalid_transition`。
  这些拒绝都不改状态、不加实例版本、不动账本与审计。

### `POST /v1/instances/{id}/recover`
显式恢复入口：把一个 **`dead_lettered`** 实例放回可推进状态。请求体**只允许空 JSON 对象 `{}`**
（无 body、非 JSON、`null`、数组、含任何字段的对象均 ⇒ `400 invalid_request`），可带可选 **`If-Match`**
（规则与 `/events` 完全一致，格式非法 ⇒ `400 invalid_request`）→
`200 {"id":..., "workflow":..., "workflowVersion":..., "state": {...}}`，成功响应带
**`ETag: "<恢复后的实例版本>"`**（恢复是真正的实例变更，版本恰好 +1）。
- 恢复只做最小复位：`status` 变为 `running`，`failure` 变为 `null`，`attempt` 重置为 `1`；
  `id`、`workflow`、`workflowVersion`、`step`、`index`、`completed`、`compensated`、`context` 原样保留。
  当前步骤有 `await` 时 `waitingFor` 设为对应事件名；该 `await` 带 `timeoutMs` 时按**恢复成功时刻**
  重新加 `timeoutMs` 生成新的 `deadlineAt`，不带则 `deadlineAt` 为 `null`；当前步骤没有 `await` 时
  `waitingFor` 与 `deadlineAt` 均为 `null`。
- 恢复**不代替**后续事件或信号，也**不触发补偿**：不重放已完成步骤、不重算任何结果、
  不重建分区分号、不改写既有幂等账本与审计历史；恢复后仍须由匹配信号或普通结果推进。
  恢复**不写幂等账本、不追加审计记录**（审计仍只收录真正推进状态机的 `/events` 与 `/signals`），
  账本与审计中已存的历史响应（含 `dead_lettered` 状态的 `timed_out` 记录）永不改写。
- 实例不存在 ⇒ `404 not_found`；实例不是 `dead_lettered`（running/completed/compensated）
  或对已恢复实例重复恢复 ⇒ **`409 invalid_transition`**，且不产生第二次变更；
  携带的 `If-Match` 与当前实例版本不相等同样 ⇒ **`409 invalid_transition`**。
  这些拒绝都不改 state、实例版本、幂等账本、分区分号与审计历史（错误响应不带 `ETag`）。
- 恢复后的实例维持当前一切语义：普通事件、匹配信号、等待超时拒绝、重试耗尽补偿、版本迁移、
  幂等回放与顺序门控行为不变；固定版本为 `NULL` 的旧实例在恢复成功时与普通推进一样补记当时最新版。
  旧 SQLite 文件照常升级读取，缺失的旧状态字段继续按现有兼容规则回填；恢复及后续变更重开同一文件后一致。

### `GET /v1/instances/{id}/audit`
实例级只读审计查询（无请求体）→ `200 {"id","workflow","workflowVersion","state","history":[...]}`，
并带响应头 **`ETag: "<当前实例版本>"`**；未知 id ⇒ `404 not_found`。
- `state` 为查询时的当前状态（与 `GET /v1/instances/{id}` 一致）；`history` 按 `seq` 从 1 开始严格递增，
  只收录**真正推进状态机**的 `/events` 与 `/signals` 调用。
- 每条记录：`{"seq","kind","eventId","request","response"}`；`kind` 为 `event`（/events）或
  `signal`（/signals），同一 `eventId` 字符串被事件与信号复用时通过 `kind` 区分。
- `request` 保存归一化输入（`eventId`/`outcome`/`detail` 或 `eventId`/`event`/`detail`）；
  未提交 `eventId` 时记录中的 `eventId` 为 `null`；省略 `detail` 与显式 `null` 都记为 `null`；
  被接受的 `timed_out` 以 `kind="event"`、`request.outcome="timed_out"` 记录，`seq` 连续递增。
  有序调用（带 `partitionKey`/`sequence`，见“持久化分区顺序”）的 `request` 额外保存
  `partitionKey` 与 `sequence`，其 `response` 含 `ordering`；无序调用与升级前旧记录均不含这些字段。
- `response` 保存该次处理返回的完整 JSON 响应，无需重新执行状态机即可逐条核对当时结果。
- 幂等回放（相同 `eventId` 相同载荷）仍只返回首次响应且只占一条审计记录，即使实例后来已终态；
  非法请求、相同 `eventId` 不同载荷、终态推进、等待期间提交事件、信号名不匹配等被拒绝的调用
  均不追加记录。不同 `eventId` 的并发接受结果按某个确定串行顺序获得连续 `seq`。
- 审计记录与状态更新、幂等账本在**同一事务**提交（SQLite 表 `instance_audit`），
  服务重开同一文件后 `history`、`seq` 与当前 `state` 保持一致。
- 新表以 `CREATE TABLE IF NOT EXISTS` 建立，旧 SQLite 文件直接可读且既有读取与回放行为不变；
  升级前的调用**不伪造补录**，旧实例从升级后的下一次成功推进开始积累历史。

## 实例版本与 If-Match（显式并发控制）

实例版本用于乐观并发抢占，避免客户端用旧观察结果覆盖已经接受的推进或迁移；它与“定义版本”
（`workflowVersion`）完全独立，不改变工作流定义、状态转移、补偿顺序、幂等规则、迁移兼容规则和既有审计。

- **版本起点**：每个实例从版本 **1** 开始。
- **暴露方式**：`GET /v1/instances/{id}`、`GET /v1/instances/{id}/audit` 与启动成功的 `201`
  都返回响应头 **`ETag: "<带双引号的十进制版本>"`**，例如 `ETag: "1"`；成功迁移与成功恢复的 `200`
  响应同样返回反映新实例版本的 `ETag`。
- **前置条件**：`POST /v1/instances/{id}/events`、`/signals`、`/migrate`、`/recover` 接受可选请求头
  **`If-Match`**，取值只能是一个带双引号的十进制版本（与 `ETag` 同形）。
  - 与**当前实例版本相等**才继续既有处理；不相等 ⇒ **`409 invalid_transition`**，
    且状态、实例版本、幂等账本、审计均不变（响应不带 `ETag`）。
  - `If-Match` 为空、含多个值（`"1", "2"`）、使用弱标记（`W/"1"`）、星号（`*`）、
    无引号数字（`1`）、前导零（`"01"`）、负数/小数或任何其他非规定格式
    ⇒ **`400 invalid_request`**，也不产生任何写入。
  - **省略 `If-Match`** 时不增加任何拒绝条件：现有成功响应、状态字段、JSON 字段与错误码保持不变。
- **版本递增**：只在真正改变实例时加 1——被接受的事件或信号推进状态机后 +1，成功迁移 +1，
  成功恢复 +1；迁移的 `state` 仍逐字段保留迁移前内容，恢复则把实例复位回 `running` 并重置
  `failure`/`attempt` 与等待字段。非法事件、终态继续推进、等待期间提交普通结果、
  信号名不匹配、未到期的 `timed_out`、不兼容迁移、对非 `dead_lettered` 实例的恢复都不加版本；
  未知实例仍返回 `404 not_found`。
- **回放优先于前置条件**：相同 `eventId` 且载荷相同的回放先于 `If-Match` 判断——
  即使携带的版本已经过期，也返回**首次完整响应**（响应体为首次处理时的历史 `state`），
  且不改变版本、状态、账本或审计；该回放响应的 `ETag` 反映**当前**实例版本。
  相同 `eventId` 携带不同载荷仍返回 `409 invalid_transition`（无论 `If-Match` 是否匹配）。
- **成功变更与回放**的响应都返回反映当前实例版本的 `ETag`。
- **持久化**：实例版本随同一 SQLite 文件重开保持一致（`instances.instance_version` 列，
  旧文件自动 `ALTER TABLE` 补齐）。旧实例按版本 **1** 读取，下一次真正变更后变为 **2**，
  不伪造历史变更次数。

## 错误语义

```json
{"error": {"code": "invalid_request|not_found|invalid_transition|internal_error", "message": "<可读说明>"}}
```

## 未实现（后续任务候选，非固定题单）

持久化恢复与重放、限流与背压、可视化查询与审计回放、失败注入测试。
