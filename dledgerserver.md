# DLedgerServer

有前面的架构介绍，我们知道 `DLedgerServer` 是整个 dledger 的核心管理类。而服务器节点的启动是由 `DLedger` 类来完成的，所以下面我们就从 `DLedger` 代码入手。

其代码实现逻辑很简单，

1. 初始化 dledger 配置，并从命令行解析出配置参数；
2. 创建并启动 dLedgerServer；
3. 注册钩子，在虚拟机退出时执行 dLedgerServer 的退出操作；

### 构造dLedgerServer

说到创建 dLedgerServer，代码如下：

```java
// DLedger.java
DLedgerServer dLedgerServer = new DLedgerServer(dLedgerConfig);
```

```java
// DLedgerServer.java
public DLedgerServer(DLedgerConfig dLedgerConfig) {
    this.dLedgerConfig = dLedgerConfig;
    this.memberState = new MemberState(dLedgerConfig);

    // 根据存储策略（FILE, MEMORY），初始化不同的存储实现。默认为FILE。
    this.dLedgerStore = createDLedgerStore(dLedgerConfig.getStoreType(), this.dLedgerConfig, this.memberState);

    dLedgerRpcService = new DLedgerRpcNettyService(this);
    dLedgerEntryPusher = new DLedgerEntryPusher(dLedgerConfig, memberState, dLedgerStore, dLedgerRpcService);
    dLedgerLeaderElector = new DLedgerLeaderElector(dLedgerConfig, memberState, dLedgerRpcService);
}
```

1. `createDLedgerStore` 方法根据不同的存储策略来初始化不同的存储实现。默认策略为FILE。

   ```java
    private DLedgerStore createDLedgerStore(String storeType, DLedgerConfig config, MemberState memberState) {
        if (storeType.equals(DLedgerConfig.MEMORY)) {
            return new DLedgerMemoryStore(config, memberState);
        } else {
            return new DLedgerMmapFileStore(config, memberState);
        }
    }
   ```

2. TODO

### 启动dLedgerServer

```java
// DLedger.java
dLedgerServer.startup();
```

```java
// DLedgerServer.java
public void startup() {
    this.dLedgerStore.startup();
    this.dLedgerRpcService.startup();
    this.dLedgerEntryPusher.startup();
    this.dLedgerLeaderElector.startup();
}
```

有关 `this.dLedgerStore.startup();` 的讲解参见 `DLedgerMmapFileStore`。

TODO

