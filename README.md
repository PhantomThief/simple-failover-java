simple-failover-java [![Build Status](https://travis-ci.org/PhantomThief/simple-failover-java.svg)](https://travis-ci.org/PhantomThief/simple-failover-java) [![Coverage Status](https://coveralls.io/repos/PhantomThief/simple-failover-java/badge.svg?branch=master)](https://coveralls.io/r/PhantomThief/simple-failover-java?branch=master)
=======================

A simple failover library for Java

* jdk1.8 only

## Get Started

```xml
<dependency>
    <groupId>com.github.phantomthief</groupId>
    <artifactId>simple-failover</artifactId>
    <version>0.1.10</version>
</dependency>
```

```Java	
List<T> orig = ... // original list

Failover<T> failover = RecoverableCheckFailover.<T> newBuilder() //
        .setFailCount(10) //
        .setFailDuration(1, TimeUnit.MINUTES) //
        .setChecker(this::test, RECOVERED_RATE) //
        .build(orig);
List<T> available = failover.getAvailable(2); // random get 2 available objects.
// or
T obj = failover.getOneAvailable(); // random get an available object.

// do something with object...

// when it fails, mark it.
failover.fail(obj);
```