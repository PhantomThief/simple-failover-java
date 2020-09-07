simple-failover-java
=======================
[![Build Status](https://travis-ci.org/PhantomThief/simple-failover-java.svg)](https://travis-ci.org/PhantomThief/simple-failover-java)
[![Coverage Status](https://coveralls.io/repos/PhantomThief/simple-failover-java/badge.svg?branch=master)](https://coveralls.io/r/PhantomThief/simple-failover-java?branch=master)
[![Total alerts](https://img.shields.io/lgtm/alerts/g/PhantomThief/simple-failover-java.svg?logo=lgtm&logoWidth=18)](https://lgtm.com/projects/g/PhantomThief/simple-failover-java/alerts/)
[![Language grade: Java](https://img.shields.io/lgtm/grade/java/g/PhantomThief/simple-failover-java.svg?logo=lgtm&logoWidth=18)](https://lgtm.com/projects/g/PhantomThief/simple-failover-java/context:java)
[![Maven Central](https://img.shields.io/maven-central/v/com.github.phantomthief/simple-failover)](https://search.maven.org/artifact/com.github.phantomthief/simple-failover/)

A simple failover library for Java.

用于构建高性能的客户端（主调方）自适应负载均衡和自动重试能力。

* jdk1.8+

## Get Started

```Java	
// 添加多个被调用资源，这里的被调用资源是指目标服务器（有多个服务器提供相同的服务），下面例子展示了3个，每个权重100
String server1 = "192.168.1.1";
String server1 = "192.168.1.2";
String server1 = "192.168.1.3";
SimpleFailover<String> failover = PriorityFailover.newBuilder()
        .addResource(server1, 100.0) 
        .addResource(server2, 100.0)
        .addResource(server3, 100.0)
        .build();
String server = failover.getOneAvailable();
// 获取一个资源执行操作，上面添加了3个，并且初始权重相同，所以刚开始的时候每个资源被选中的概率是1/3
boolean success = doSomethingWithServer(server);
if(success) {
    // 成功会增加权重，最大不会超过上面指定的100
    failover.success(server);
} else {
    // 失败扣减权重，降低下次被选择的概率，最小扣到0
    failover.fail(server);

    // 如果需要重试1次
    server = failover.getOneAvailableExclude(Collections.singleton(server));
    doSomethingWithServer(server);
    // ...
}
```

## 性能
本类库设计用来支撑百万TPS（单主调方）级别的RPC client的构建，因此它本身要达到千万TPS级别才能保证不成为瓶颈。

* 当被调资源全部完全健康（当前权重=最大权重）且最大权重相同时，使用round robin算法，时间复杂度O(1)
* 当被调资源数>10，最大权重不同，且全部完全健康时，使用AliasMethod算法，时间复杂度O(1)
* 其它情况下时间复杂度为O(N)。当开启了优先级分组以后，N为最高优先级的资源数，这个数字通常较小，从而大幅提升了性能。

核心类PriotiryFailover是线程安全的，无锁实现保证高性能。
PriorityFailoverManager/PriorityGroupManager的变更方法（通常资源上下线才使用）都不是线程安全的，变更时请自行加锁，PriorityFailoverBuilder类一般来说是使用完就丢弃的，也不是线程安全的。

单元测试代码中BenchmarkMain类可以用来做性能测试，文末有测试结果表，不同的工况下有不同的性能数值，整个表格中只有单线程N较大的一个场景下没有达标，大部分场景都可以达到数千万TPS。


## cookbook
### 定制权重增加和扣减策略
默认情况下，每次调用失败将当前权重减半，例如：
初始权重100.0，第一次失败后（调用fail方法后）权重为50.0，第二次失败后25.0，第三次失败后12.5。

每次成功后将增加最大权重的1%，例如：
最大权重100，当前权重12.5，成功后（调用success方法后），权重为13.5。

所以这个默认算法是等比递减、等差递增。等比递减可以保证被调用服务器不稳定的情况下权重迅速下降（尽快摘除）。如果需要调整参数：
```java
PriorityFailoverBuilder<String> buidler = PriorityFailover.newBuilder();
// 每次失败后只保留当前权重的0.1，成功后增加最大权重的0.01
buidler.weightFunction(new RatioWeightFunction(
          /*failKeepRateOfCurrentWeight*/0.1, /*successIncreaseRateOfMaxWeight*/0.01));
```

如果想等差扣减权重：
```java
// 每次失败后扣酱最大权重的0.05，成功后增加最大权重的0.01
buidler.weightFunction(new SimpleWeightFunction(
          /*failDecreaseRate*/0.05, /*successIncreaseRate*/0.01));
```

### 主动健康检查
不健康的资源，通过后台线程运行主动健康检查恢复权重，需要设置检查器和检查间隔参数：
```java
buidler.checker(server -> pingServer(server));
builder.checkDuration(Duration.ofSeconds(60));
```
默认情况下，权重变成0算作不健康，健康检查成功1次就立刻恢复权重。这个行为可以通过WeightFunction定制：
```java
buidler.weightFunction(new RatioWeightFunction(/*failKeepRateOfCurrentWeight*/0.5, 
        /*successIncreaseRateOfMaxWeight*/0.01, /*recoverThreshold*/2, /*downThreshold*/0.1));
```
以上第一个参数指定每次调用失败后权重减半，第二个参数指定调用成功后增加最大权重的0.01，第三个参数指定调用主动健康检查连续成功两次才可以增加权重（增加的数值由第二个参数指定），第四个参数指定当前权重小于0.1以后直接变成为0（否则等比递减很难减少到0，此时不会有主动健康检查）。

如果希望完全健康的时候也执行健康检查也可以通过weightFunction覆盖needCheck方法来定制。
另外检查任务是懒启动的，需要有一次fail或者down的调用才会启动，如果不想这样（例如完全健康的节点也想定期检查），可以在builder上设置startCheckTaskImmediately。

### 最小权重
一般来说最小权重就是0，一个资源连续调用失败，它的权重应该被扣减到0，以免新的请求发到它那里去。

但有的时候我们不想这样，比如，我们总共只有两个集群，希望无论如何都选择一个相对健康的集群发送请求，可以通过以下代码指定最小权重是0.1：
```java
SimpleFailover<String> failover = PriorityFailover.newBuilder()
        .addResource(server1, /*maxWeight*/100.0, /*minWeight*/0.1) 
...
```

### 运行时变更资源列表
主调方已经启动的情况下，被调资源有变动，这在分布式rpc场景是很常见的，可以通过
```java
Object server1 = "s1";
Object server2 = "s2";
Object server3 = "s3";
PriorityFailoverManager<Object> manager = PriorityFailover.newBuilder()
        .addResource(server1, 100)
        .addResource(server2, 100)
        .buildManager();
Object obj = manager.getFailover().getOneAvailable();
// 总是通过manager拿到PriorityFailover来用

// 当资源需要变更的时候
HashMap<Object, ResConfig> map = new HashMap<>();
map.put(server2, new ResConfig(/*maxWeight*/100));
map.put(server3, new ResConfig(/*maxWeight*/100));
manager.updateAll(map); //还有个update方法可以做增量更新

// 之后manager.getFailover()取出来的是个新的failover，删除了server1，添加了server3
```
变更后原有资源（上面的例子是server2）的权重会从之前的failover继承下来。updateAll的时候最大权重等参数可以改。

以上代码展示了全量变更，通过update方法可以实现增量变更。

### 预热/慢启动
上面的代码中，server3是个新加的资源，为了防止新上线的实例被打爆，我们可以把它的初始权重设置为5。
```java
map.put(server3, new ResConfig(/*maxWeight*/100, /*minWeight*/0, /*priority*/0, /*initWeight*/5));
```
这样，新上的server3只有5%的流量，随着调用的成功，它的权重会慢慢增加到100。

### 并发度控制（自适应负载均衡）
多个被调资源的性能能力可能是不一样的，可能是因为硬件性能不同，也可能是某个资源上还运行了其它服务等原因。
虽然我们可以在addResource的时候为它们设置不同的最大权重，但是复杂场景下我们很难估计好不同资源之间的最大权重比，何况资源的能力可能在运行时变化。
假设现在有个被调资源在有性能压力，刚开始不会失败，如果持续高流量进入，会导致这个对这个资源的调用开始出错。所以我们希望，当某个资源有性能问题的苗头时，就减少对它的流量。

这个性能问题可以通过响应时间反应出来，在我们的代码中也就是调用getOneAvailable和调用success/fail的时间差。所以我们引入并发度的概念：
```java
builder.concurrencyControl(true);
```
通过以上代码激活并发度控制，并发度高的资源，流量会减少。
并发度初始值是0，getOneAvailable会将并发度加1，success/fail/down会将并发度减1，内部选择资源的时候，当前权重会除以（并发度+1），也就是说如果并发度为1，有效权重就会减半（除以2），并发度是2时，有效权重就会变为1/3。

有个要求是getOneAvailable取出来的资源用完后，必须通过success/fail/down放回去，否则并发度计算会错误。

### 资源优先级
被调资源很多的情况下（比如有1000台服务器），我们不想把请求均匀的发到这些服务器上，因为这会导致：
1. 导致底下的网络长连接、连接池不能很好的复用
2. 如果主调方的访问的数据有局部性，可能导致被调方缓存命中率降低
3. 被调方和主调方可能位置相关的特性，比如机房、可用区，希望调用时本地优先

构建的时候：
```java
.addResource(server1, /*maxWeight*/100, /*minWeight*/0, /*priority*/0, /*initWeight*/100))
.addResource(server2, /*maxWeight*/100, /*minWeight*/0, /*priority*/0, /*initWeight*/100))
.addResource(server3, /*maxWeight*/100, /*minWeight*/0, /*priority*/1, /*initWeight*/100))
.addResource(server4, /*maxWeight*/100, /*minWeight*/0, /*priority*/1, /*initWeight*/100))
```
以上4个服务器分为2个group，当优先级为0的group变的不健康以后，流量会逐渐溢出到优先级为1的group。具体可看PriorityFailover设计。

注意流量是逐渐溢出的，具体算法与[envoy](https://www.servicemesher.com/envoy/intro/arch_overview/load_balancing.html)类似，默认因子也是1.4，如果想要第一个group里面的资源死光光以后再溢出，可以在builder上设置：
```java
builder.priorityFactor(Double.MAX_VALUE)
```
### 自动优先级管理
比如，机房里有300个RPC被调资源，想分为2组，第一组5个，剩下的归为第二组：
```java
builder.enableAutoPriority(5);
```

如果希望分3组，第一个组5个，第二组20个，剩下的归为第三组，可以这样：
```java
builder.enableAutoPriority(5, 20)
```

PriorityFailoverManager也能够支持自动优先级管理。

使用自动优先级管理，资源变更时，新增的资源和已有资源一样有均等的机会进入高优先级组；
同时已有的资源本来会在高优先级组的仍然会优先，以尽量保持调用的粘性，减少变更的影响，详见PriorityGroupManager类。

### 资源事件通知（比如资源down）
可以在builder上设置weightListener：

```java
public interface WeightListener<T> {
    void onSuccess(double maxWeight, double minWeight, double currentNewWeight, T resource);

    void onFail(double maxWeight, double minWeight, double currentNewWeight, T resource);
}
```
需要注意这个回调仅在资源权重变更的时候触发（完全健康状态下再次成功是没有onSuccess回调的，onFail也类似）。


## benchmark测试结果表

* 测试环境，笔记本，I7-9750H，6核12线程，2.6G~4.5G
* Score数值为千TPS，即要乘以1000。
* concurrencyCtrl代表是否打开并发度控制，totalSize代表资源数，coreSize代表分2组优先级情况下，priority=0的资源数，剩下的priority=1。
* getOneSuccess代表getOneAvailable获取资源以后，接下来调用failover.success(res)方法，模拟资源调用成功。
* getOneFail代表getOneAvailable获取资源以后，模拟1/1000的资源调用失败。资源调用失败会造成性能failover劣化，时间复杂度会退化到O(N)，同时多线程场景下对volatile变量的写也会导致性能降低。

1线程：
```
Benchmark                             (concurrencyCtrl)  (coreSize)  (totalSize)   Mode  Cnt      Score   Error  Units
Group1PriorityFailover.getOneFail                   N/A         N/A            5  thrpt    2  66408.751          ops/s
Group1PriorityFailover.getOneFail                   N/A         N/A           20  thrpt    2  68001.154          ops/s
Group1PriorityFailover.getOneFail                   N/A         N/A          100  thrpt    2  47727.860          ops/s
Group1PriorityFailover.getOneFail                   N/A         N/A          200  thrpt    2  25812.804          ops/s
Group1PriorityFailover.getOneFail                   N/A         N/A         1000  thrpt    2   2433.041          ops/s
Group1PriorityFailover.getOneSuccess                N/A         N/A            5  thrpt    2  95741.988          ops/s
Group1PriorityFailover.getOneSuccess                N/A         N/A           20  thrpt    2  92530.019          ops/s
Group1PriorityFailover.getOneSuccess                N/A         N/A          100  thrpt    2  85027.270          ops/s
Group1PriorityFailover.getOneSuccess                N/A         N/A          200  thrpt    2  84072.456          ops/s
Group1PriorityFailover.getOneSuccess                N/A         N/A         1000  thrpt    2  56069.687          ops/s
Group2PriorityFailover.getOneFail                  true           5         1000  thrpt    2  17725.844          ops/s
Group2PriorityFailover.getOneFail                  true          20         1000  thrpt    2   9911.852          ops/s
Group2PriorityFailover.getOneFail                 false           5         1000  thrpt    2  66638.774          ops/s
Group2PriorityFailover.getOneFail                 false          20         1000  thrpt    2  65462.617          ops/s
Group2PriorityFailover.getOneSuccess               true           5         1000  thrpt    2  17721.040          ops/s
Group2PriorityFailover.getOneSuccess               true          20         1000  thrpt    2  10192.638          ops/s
Group2PriorityFailover.getOneSuccess              false           5         1000  thrpt    2  84482.401          ops/s
Group2PriorityFailover.getOneSuccess              false          20         1000  thrpt    2  87764.225          ops/s
```

5线程：
```
Benchmark                             (concurrencyCtrl)  (coreSize)  (totalSize)   Mode  Cnt      Score   Error  Units
Group1PriorityFailover.getOneFail                   N/A         N/A            5  thrpt    2  35019.174          ops/s
Group1PriorityFailover.getOneFail                   N/A         N/A           20  thrpt    2  29588.433          ops/s
Group1PriorityFailover.getOneFail                   N/A         N/A          100  thrpt    2  24206.241          ops/s
Group1PriorityFailover.getOneFail                   N/A         N/A          200  thrpt    2  26622.312          ops/s
Group1PriorityFailover.getOneFail                   N/A         N/A         1000  thrpt    2  13794.910          ops/s
Group1PriorityFailover.getOneSuccess                N/A         N/A            5  thrpt    2  39493.298          ops/s
Group1PriorityFailover.getOneSuccess                N/A         N/A           20  thrpt    2  37636.730          ops/s
Group1PriorityFailover.getOneSuccess                N/A         N/A          100  thrpt    2  39736.684          ops/s
Group1PriorityFailover.getOneSuccess                N/A         N/A          200  thrpt    2  40909.686          ops/s
Group1PriorityFailover.getOneSuccess                N/A         N/A         1000  thrpt    2  44379.175          ops/s
Group2PriorityFailover.getOneFail                  true           5         1000  thrpt    2  13332.955          ops/s
Group2PriorityFailover.getOneFail                  true          20         1000  thrpt    2  12736.631          ops/s
Group2PriorityFailover.getOneFail                 false           5         1000  thrpt    2  30005.073          ops/s
Group2PriorityFailover.getOneFail                 false          20         1000  thrpt    2  29428.004          ops/s
Group2PriorityFailover.getOneSuccess               true           5         1000  thrpt    2  15578.987          ops/s
Group2PriorityFailover.getOneSuccess               true          20         1000  thrpt    2  20515.372          ops/s
Group2PriorityFailover.getOneSuccess              false           5         1000  thrpt    2  38486.222          ops/s
Group2PriorityFailover.getOneSuccess              false          20         1000  thrpt    2  37179.500          ops/s
```

200线程
```
Benchmark                             (concurrencyCtrl)  (coreSize)  (totalSize)   Mode  Cnt      Score   Error  Units
Group1PriorityFailover.getOneFail                   N/A         N/A            5  thrpt    2  45083.860          ops/s
Group1PriorityFailover.getOneFail                   N/A         N/A           20  thrpt    2  43295.450          ops/s
Group1PriorityFailover.getOneFail                   N/A         N/A          100  thrpt    2  39304.540          ops/s
Group1PriorityFailover.getOneFail                   N/A         N/A          200  thrpt    2  39092.222          ops/s
Group1PriorityFailover.getOneFail                   N/A         N/A         1000  thrpt    2  26619.782          ops/s
Group1PriorityFailover.getOneSuccess                N/A         N/A            5  thrpt    2  60798.031          ops/s
Group1PriorityFailover.getOneSuccess                N/A         N/A           20  thrpt    2  59382.891          ops/s
Group1PriorityFailover.getOneSuccess                N/A         N/A          100  thrpt    2  60453.622          ops/s
Group1PriorityFailover.getOneSuccess                N/A         N/A          200  thrpt    2  58952.071          ops/s
Group1PriorityFailover.getOneSuccess                N/A         N/A         1000  thrpt    2  55245.930          ops/s
Group2PriorityFailover.getOneFail                  true           5         1000  thrpt    2  17456.668          ops/s
Group2PriorityFailover.getOneFail                  true          20         1000  thrpt    2  19482.938          ops/s
Group2PriorityFailover.getOneFail                 false           5         1000  thrpt    2  41700.014          ops/s
Group2PriorityFailover.getOneFail                 false          20         1000  thrpt    2  40878.123          ops/s
Group2PriorityFailover.getOneSuccess               true           5         1000  thrpt    2  23196.162          ops/s
Group2PriorityFailover.getOneSuccess               true          20         1000  thrpt    2  31493.098          ops/s
Group2PriorityFailover.getOneSuccess              false           5         1000  thrpt    2  54561.672          ops/s
Group2PriorityFailover.getOneSuccess              false          20         1000  thrpt    2  53995.894          ops/s
``` 
