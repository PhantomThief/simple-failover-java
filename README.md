simple-failover-java
=======================
[![Build Status](https://travis-ci.org/PhantomThief/simple-failover-java.svg)](https://travis-ci.org/PhantomThief/simple-failover-java)
[![Coverage Status](https://coveralls.io/repos/PhantomThief/simple-failover-java/badge.svg?branch=master)](https://coveralls.io/r/PhantomThief/simple-failover-java?branch=master)
[![Total alerts](https://img.shields.io/lgtm/alerts/g/PhantomThief/simple-failover-java.svg?logo=lgtm&logoWidth=18)](https://lgtm.com/projects/g/PhantomThief/simple-failover-java/alerts/)
[![Language grade: Java](https://img.shields.io/lgtm/grade/java/g/PhantomThief/simple-failover-java.svg?logo=lgtm&logoWidth=18)](https://lgtm.com/projects/g/PhantomThief/simple-failover-java/context:java)
[![Maven Central](https://img.shields.io/maven-central/v/com.github.phantomthief/simple-failover)](https://search.maven.org/artifact/com.github.phantomthief/simple-failover/)

A simple failover library for Java

* jdk1.8 only

## Get Started

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
