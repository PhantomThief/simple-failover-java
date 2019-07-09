simple-failover-java [![Build Status](https://travis-ci.org/PhantomThief/simple-failover-java.svg)](https://travis-ci.org/PhantomThief/simple-failover-java) [![Coverage Status](https://coveralls.io/repos/PhantomThief/simple-failover-java/badge.svg?branch=master)](https://coveralls.io/r/PhantomThief/simple-failover-java?branch=master)
=======================

A simple failover library for Java

* jdk1.8 only

## Get Started

```xml
<dependency>
    <groupId>com.github.phantomthief</groupId>
    <artifactId>simple-failover</artifactId>
    <version>0.1.16</version>
</dependency>
```

```Java	
class MyClientFailoverHolder {

  private final Failover<MyClient> failover = WeightFailover.<MyClient> newGenericBuilder()
          .checker(this::checkAlive)
          .build(allClients(), 100);

  private List<MyClient> allClients() {
    // ...
  }

  private double checkAlive(MyClient myClient) {
    if (checkAlive(myAlive)) {
      return 1.0D;
    } else {
      return 0.0D;
    }
  }

  public void foo() {
    MyClient client = failover.getOneAvailable();
    try {
      client.doSomething();
      failover.success(client);
    } catch (Throwable e) {
      failover.fail(client);
      throw e;
    }
  }

  public void foo2() {
    failover.runWithRetry(MyClient::doSomething);
  }
}
```
