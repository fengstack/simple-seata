# Seata AT 模式学习指南（基于本仓库源码）

> 本文面向第一次接触分布式事务的同学。不需要先读 Seata 官方文档，
> 跟着本文的顺序，对照仓库里的真实代码走一遍，就能搞明白 AT 模式的核心原理。
> 所有代码路径都以本仓库为准，可以直接点开对照阅读。

---

## 0. 五分钟搞懂 AT 模式是什么

一句话：**AT 模式 = 一阶段就把本地事务提交掉，把"还原数据需要的信息"（undo_log）一起存进数据库；二阶段成功就删掉这份日志，失败就用它把数据改回去。**

传统的 XA 两阶段提交，一阶段后数据库把行锁一直攥在手里等二阶段指令，并发能力很差。
AT 模式的聪明之处在于：

- **一阶段**：业务 SQL 执行完，本地事务立刻提交，行锁马上释放给数据库其他人用。
  提交前多干三件事：记录改动前的数据（前镜像）、记录改动后的数据（后镜像）、
  把前后镜像写进同库的 `undo_log` 表——**和业务数据在同一个本地事务里原子提交**。
- **二阶段提交**（业务成功）：数据都已经落库了，只需要异步批量删掉 `undo_log`，几乎零开销。
- **二阶段回滚**（业务失败）：读出 `undo_log` 里的前镜像，生成一条反向 SQL 把数据改回去。

"改回去"靠什么保证不出错？两个机制：**全局行锁**（防止两个全局事务同时改一行）+
**后镜像校验**（回滚前核对数据没被别人动过，动过就拒绝回滚，即"防脏写"）。

### 三个角色

| 角色 | 全称 | 本仓库对应代码 | 职责 |
|------|------|----------------|------|
| **TM** | Transaction Manager | `seata-tm` 模块 | 决定"什么时候开全局事务、什么时候提交/回滚"的业务侧发起者 |
| **RM** | Resource Manager | `seata-rm-datasource` 模块 | 管理数据库资源：拦截 SQL、记录镜像、写 undo_log、执行补偿 |
| **TC** | Transaction Coordinator | `seata-server` 模块 | 独立的"大脑"：维护全局事务状态、分配 XID、管理全局行锁、驱动二阶段 |

一次典型调用（本仓库 examples 演示的下单场景）：

```
BusinessService (TM)
   │ begin ──────────────► TC 创建全局事务, 返回 XID
   │
   ├──► StorageService 扣库存 (RM, 库存库)   ── 分支事务 1
   ├──► AccountService 扣账户 (RM, 账户库)   ── 分支事务 2
   │
   │ commit / rollback ──► TC 驱动两个分支做二阶段
```

**XID**（全局事务 id，格式 `ip:port:transactionId`，见 `seata-common` 的
[XID.java](../seata-common/src/main/java/io/spring2go/seata/common/XID.java)）
通过 `RootContext`（ThreadLocal 实现，见
[RootContext.java](../seata-core/src/main/java/io/spring2go/seata/core/context/RootContext.java)）
在调用链里传递，让每个 RM 都知道自己属于哪个全局事务。

---

## 1. 先跑起来看现象（10 分钟）

### 看得见的演示

```bash
mvn -pl seata-examples -am install -DskipTests
mvn -pl seata-examples dependency:build-classpath -Dmdep.outputFile=/tmp/cp.txt -DincludeScope=runtime
java -cp "seata-examples/target/classes:$(cat /tmp/cp.txt)" io.spring2go.seata.examples.DemoApp
```

DemoApp 会分三幕演示，每一步都把两个库的数据和 undo_log 的**原始 JSON**（前后镜像）打出来：

1. **场景一：下单成功**——库存库/账户库各提交一个分支事务，你能亲眼看到 undo_log 长什么样，
   然后看着 AsyncWorker 每秒一次把它批量删掉（二阶段提交 = 删日志）。
2. **场景二：下单失败**——业务异常触发全局回滚，数据被反向 SQL 自动还原。
3. **场景三：脏写检测**——绕过 Seata 直接改库里的数据，回滚时后镜像校验发现数据对不上，
   **拒绝盲目补偿**；人工修复后 TC 的超时检查线程自动重试、完成补偿。

### 自动化测试

[EndToEndIntegrationTest.java](../seata-examples/src/test/java/io/spring2go/seata/examples/EndToEndIntegrationTest.java)
覆盖同样四个场景（提交/回滚/全局锁冲突/脏写检测），每个断言都有中文注释，是最好的"可执行文档"。

---

## 2. 一阶段：一次业务 SQL 背后发生了什么

以 `UPDATE storage_tbl SET count = count - 10 WHERE commodity_code = 'C001'` 为例。
入口是业务代码里的 `dataSourceProxy.getConnection()`——注意拿到的是**代理**连接，不是真连接。

### 2.1 SQL 是怎么被拦截的

```
业务代码 conn.prepareStatement(sql)
    → AbstractConnectionProxy.prepareStatement()          [返回 PreparedStatementProxy]
    → PreparedStatementProxy.executeUpdate()
    → ExecuteTemplate.execute()                            ★ AT 模式的总入口
```

代码：[ExecuteTemplate.java](../seata-rm-datasource/src/main/java/io/spring2go/seata/rm/datasource/exec/ExecuteTemplate.java)

```java
if (!RootContext.inGlobalTransaction()) {
    return statementCallback.execute(statementProxy.getTargetStatement(), args);  // 没有全局事务 → 透传
}
// 在全局事务里 → 用 Druid 解析 SQL 语法树，按类型挑一个执行器
switch (sqlRecognizer.getSQLType()) {
    case UPDATE: executor = new UpdateExecutor<>(...); break;
    case INSERT: ...
}
```

**关键认知**：AT 对业务零侵入的秘密就是 JDBC 代理 + SQL 解析。
Druid 的 AST 把 `UPDATE storage_tbl ... WHERE commodity_code='C001'` 解析出
表名和 WHERE 条件（见 `MySQLUpdateRecognizer`），RM 因此知道该去查哪些行。

### 2.2 前镜像 → 执行 → 后镜像

代码：[AbstractDMLBaseExecutor.java](../seata-rm-datasource/src/main/java/io/spring2go/seata/rm/datasource/exec/AbstractDMLBaseExecutor.java)

```java
protected T executeAutoCommitFalse(Object[] args) throws Throwable {
    TableRecords beforeImage = beforeImage();   // ① 执行前：查出将被修改的行 → 前镜像
    T result = statementCallback.execute(statementProxy.getTargetStatement(), args);  // ② 执行业务 SQL
    TableRecords afterImage = afterImage(beforeImage);  // ③ 执行后：再查一遍 → 后镜像
    statementProxy.getConnectionProxy().prepareUndoLog(...);  // ④ 前后镜像暂存到连接上下文
    return result;
}
```

前镜像查询是 RM 自己拼的 `SELECT id, count FROM storage_tbl WHERE commodity_code = 'C001'`
（`UpdateExecutor.buildBeforeImageSQL`），不是凭空猜的。

### 2.3 commit 拦截：分支注册 + undo_log 落库 + 本地提交

业务代码调 `conn.commit()` 时，真正的主角登场：
[ConnectionProxy.commit()](../seata-rm-datasource/src/main/java/io/spring2go/seata/rm/datasource/ConnectionProxy.java)

```java
public void commit() throws SQLException {
    if (context.inGlobalTransaction()) {
        register();                        // ① 向 TC 注册分支事务，同时申请全局行锁
        UndoLogManager.flushUndoLogs(this);// ② 把前后镜像序列化成 JSON，INSERT 进 undo_log 表
        targetConnection.commit();         // ③ 真正的本地事务提交：业务数据 + undo_log 一起落库！
        report(true);                      // ④ 向 TC 上报分支状态 PhaseOne_Done
    } else {
        targetConnection.commit();         // 普通事务，直接提交
    }
}
```

**为什么 undo_log 必须和业务数据在同一个本地事务里提交？**
保证原子性：要么"业务改动 + 回滚依据"同时存在，要么都不存在。
不存在"业务改了但 undo_log 丢了"（无法回滚）或"undo_log 在但业务没改"（错误回滚）的状态。

### 2.4 全局行锁：分支注册时申请

`register()` → TC 的 [DefaultCoordinator.handle(BranchRegisterRequest)](../seata-server/src/main/java/io/spring2go/seata/server/coordinator/DefaultCoordinator.java)：

```java
boolean lockAcquired = lockManager.acquireLock(branchSession);  // 申请行锁
if (!lockAcquired) {
    response.setTransactionExceptionCode(TransactionExceptionCode.LockKeyConflict);  // 锁冲突
    ...
}
```

锁的粒度是**行**，锁的 key 是 `resourceId^^^表名^^^主键值`，实现简单到一屏读完：
[LockManager.java](../seata-server/src/main/java/io/spring2go/seata/server/lock/LockManager.java)
—— 一个 `ConcurrentMap<String, Long>`（行 → 持有的事务 id）。

**全局锁解决什么问题**？数据库本地锁在一阶段提交后就释放了，如果没有全局锁，
两个全局事务 A、B 可以先后改同一行并都提交一阶段；A 回滚时按 A 的前镜像还原，
就把 B 已提交的改动覆盖掉了（脏写）。全局锁保证**同一时刻只有一个全局事务**能改某一行，
锁在 TC 侧持有到全局事务结束（提交或回滚）才释放。

锁冲突时 RM 侧会**先回滚本地事务再重试**（`AbstractDMLBaseExecutor.executeAutoCommitTrue`
的 catch 块，重试预算由 `LockRetryController` 控制，超时抛 `LockWaitTimeoutException`）。
注意每轮重试前回滚是必须的——否则重试会把业务 SQL 再执行一遍（扣两次库存）。

---

## 3. 二阶段提交：为什么"快"

TM 侧业务全部成功后 `tx.commit()` →
[DefaultCoordinator.handle(GlobalCommitRequest)](../seata-server/src/main/java/io/spring2go/seata/server/coordinator/DefaultCoordinator.java)
遍历所有分支，向 RM 发 `BranchCommitRequest`。

RM 收到后（[RMHandlerAT.doBranchCommit](../seata-rm-datasource/src/main/java/io/spring2go/seata/rm/RMHandlerAT.java)
→ [AsyncWorker](../seata-rm-datasource/src/main/java/io/spring2go/seata/rm/datasource/AsyncWorker.java)）：
**什么都不改**，只是把 (xid, branchId) 塞进内存队列，立即返回成功。
后台线程每秒批量 `DELETE FROM undo_log WHERE ...`。

因为一阶段业务数据已经提交了，二阶段提交只剩"打扫卫生"，
所以 AT 的正常路径性能接近没有分布式事务的样子——这就是"异步删除 undo_log"的含义。

---

## 4. 二阶段回滚：反向补偿是怎么构造的

TM 侧任何异常 → `tx.rollback()` → TC **按分支注册的逆序**逐个发 `BranchRollbackRequest`
（逆序很重要：最后改的数据最先还原，和数据库回滚的直觉一致）。

RM 侧链路（三个类，建议按此顺序读）：

1. [RMHandlerAT.doBranchRollback](../seata-rm-datasource/src/main/java/io/spring2go/seata/rm/RMHandlerAT.java)
   —— 接指令，按 resourceId 找到对应的 DataSourceProxy。
2. [UndoLogManager.undo()](../seata-rm-datasource/src/main/java/io/spring2go/seata/rm/datasource/undo/UndoLogManager.java)
   —— 开一个新本地事务：`SELECT ... FROM undo_log ... FOR UPDATE` 读出 JSON 反序列化，
   交给 UndoExecutor 执行，成功后删 undo_log 并提交。
3. [AbstractUndoExecutor](../seata-rm-datasource/src/main/java/io/spring2go/seata/rm/datasource/undo/AbstractUndoExecutor.java)
   —— 先做**后镜像校验（防脏写）**，再用前镜像构造**反向 SQL**：

| 一阶段的操作 | undo_log 保留的镜像 | 反向 SQL |
|--------------|--------------------|----------|
| UPDATE（100→90） | beforeImage: count=100 | `UPDATE storage_tbl SET count=100 WHERE id=1` |
| INSERT（新增行） | afterImage: 整行数据 | `DELETE FROM storage_tbl WHERE id=?` |
| DELETE（删除行） | beforeImage: 整行数据 | `INSERT INTO storage_tbl (...) VALUES (...)` |

（SQL 拼装见 `MySQLUndoUpdateExecutor` / `MySQLUndoInsertExecutor` / `MySQLUndoDeleteExecutor`。）

### 防脏写校验（本仓库在 `AbstractUndoExecutor.dataValidation` 实现）

回滚前先按主键查当前数据，和**后镜像**逐字段比对：

- 一致 → 没人动过，放心执行反向 SQL；
- 不一致 → 这行数据在全局事务之外被改过（脏写）。**拒绝补偿**，返回
  `PhaseTwo_RollbackFailed_Retriable`。盲目补偿会用旧值覆盖别人的修改，宁可停下来。

TC 收到失败后把全局事务置为 `RollbackRetrying`、**不释放锁、不删会话**，
由超时检查线程（`DefaultCoordinator.timeoutCheck()`）每秒重试；
数据被人工修复成后镜像的应然状态后，重试自然通过，补偿完成。
DemoApp 场景三完整演示了这条故障路径。

---

## 5. TC：全局事务的"大脑"怎么运转

[DefaultCoordinator](../seata-server/src/main/java/io/spring2go/seata/server/coordinator/DefaultCoordinator.java)
是 TC 的核心，全部逻辑集中在几个 handle 方法里：

- `handle(GlobalBeginRequest)`：生成 XID，创建 `GlobalSession`（见 `SessionManager`，纯内存 Map）。
- `handle(BranchRegisterRequest)`：登记 `BranchSession`，申请行锁。
- `handle(GlobalCommitRequest)` / `handle(GlobalRollbackRequest)`：驱动二阶段。
  **任何一个分支失败都不会宣布成功**——会话留在 `CommitRetrying` / `RollbackRetrying` 等重试。
- `timeoutCheck()`（由 [EmbeddedTC](../seata-server/src/main/java/io/spring2go/seata/server/EmbeddedTC.java)
  的定时线程每秒调用）：
  1. `Begin` 状态超过 timeout 还没结束的（TM 宕机了）→ 自动回滚，防止行锁永久占用；
  2. Retrying 状态的 → 继续重试二阶段。

TM/RM 与 TC 之间走 Netty 自定义二进制协议（魔数 `0xdada`、14 字节定长头、消息合并），
编解码实现在 [MessageCodecHandler.java](../seata-core/src/main/java/io/spring2go/seata/core/rpc/netty/MessageCodecHandler.java)，
想深入网络层的同学可以从它的 `encode/decode` 入手。

---

## 6. 建议的断点调试路线（IDE 里走一遍）

跑 `EndToEndIntegrationTest#testTwoPhaseCommitSuccess`，依次在这些问题上停下来观察：

| 断点位置 | 观察什么 |
|----------|----------|
| `ExecuteTemplate.execute` | 透传 vs 进入执行器的分叉；SQLRecognizer 解析结果 |
| `UpdateExecutor.beforeImage/afterImage` | 两次 SELECT 查回来的 TableRecords 内容 |
| `ConnectionProxy.commit` | register→flushUndoLogs→commit→report 的顺序；此时数据库里 undo_log 已有数据 |
| `DefaultCoordinator.handle(BranchRegisterRequest)` | LockManager 里 lockMap 的变化 |
| `DefaultCoordinator.handle(GlobalCommitRequest)` | 二阶段指令如何逐分支发出 |
| `AsyncWorker.doBranchCommits` | 每秒一次的批量 DELETE |
| `AbstractUndoExecutor.dataValidation`（跑 testDirtyWriteDetectionAndRecovery） | 比对失败如何抛出、TC 如何转 RollbackRetrying |

---

## 7. 常见疑问（FAQ）

**Q1：一阶段就提交了本地事务，那"全局事务还没提交"期间，别人读到的是中间状态吗？**
是的。AT 默认读未提交（全局维度），靠全局锁保证**写-写**冲突，
但**读**不受全局锁限制。这也是 AT 与 XA（强隔离）的核心取舍：用隔离性换性能。

**Q2：undo_log 和业务库必须同库吗？**
必须。二阶段回滚要用反向 SQL 改业务库的数据，undo_log 里的前后镜像也必须和业务数据
原子绑定（同一个本地事务），跨库就破坏了这层保证。所以**每个参与全局事务的库都要建 undo_log 表**
（examples 里两个库各有一张，DDL 在 `storage-schema.sql` / `account-schema.sql`）。

**Q3：undo_log 会不会越积越多？**
正常不会：提交路径由 AsyncWorker 每秒批量删；回滚路径补偿成功即删。
残留一般意味着二阶段失败（比如脏写），TC 会一直 Retrying，需要人工处理。

**Q4：全局锁和数据库行锁是什么关系？**
两层锁：数据库行锁管到一阶段本地提交为止（事务结束即释放）；
全局锁是 TC 内存里的逻辑锁，从分支注册持有到全局事务结束。
全局锁冲突 → RM 回滚本地事务 → 重试（见 2.4），重试预算耗尽抛 `LockWaitTimeoutException`。

**Q5：和真实 Seata 比，这个教学实现简化了什么？**
主要有：TC 会话/锁纯内存（真实版有文件/DB 持久化和集群）、只支持 MySQL 方言、
无 Spring 注解集成（用编程式 `TransactionalTemplate`，反而更适合看清流程）、
无注册中心（TC 地址写死在 application.conf）。核心协议与 AT 语义是一致的。

---

## 8. 一页纸总结

```
一阶段（每个分支，RM 自动做）：
  拦截 SQL → 解析出影响的行 → 查前镜像 → 执行 → 查后镜像
  → 向 TC 注册分支 + 申请全局行锁
  → undo_log(前后镜像) 与业务数据同一本地事务提交
  → 上报 PhaseOne_Done

二阶段提交（TM 确认成功）：
  TC 逐分支通知 → RM 只是把 (xid, branchId) 入队
  → AsyncWorker 每秒批量 DELETE undo_log        ★ 快的原因

二阶段回滚（TM 抛异常）：
  TC 按注册逆序逐分支通知 → RM 读 undo_log
  → 校验当前数据 == 后镜像（防脏写，不一致拒绝补偿并 Retrying）
  → 用前镜像生成反向 SQL 还原数据 → 删 undo_log

兜底（TC 超时检查线程，每秒）：
  TM 宕机导致超时的 Begin 会话 → 自动回滚
  RollbackRetrying / CommitRetrying → 持续重试
```

祝学习顺利。遇到代码里看不懂的类，回到本文找它所在的环节，先看"它在哪一步被调用"，再看"它做了什么"。
