/*
 * Copyright (C) 2011 Couchbase, Inc.
 * All rights reserved.
 */
package com.couchbase.demo;

import com.couchbase.client.CouchbaseClient;
import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.logging.Level;
import java.util.logging.Logger;
import net.spy.memcached.PersistTo;
import net.spy.memcached.internal.OperationFuture;
import net.spy.memcached.ops.OperationStatus;

/**
 *
 * SleepingTester is designed to sleep for a random period of time and then
 * issue various operations to the server, much like a real user's code would
 * when running in an app server. During the sleepy time, a real application
 * would be sending HTML to a user, calling other databases and web services, or
 * filling the log file.
 *
 * There are many places here where we should have better error handling.
 *
 * @author Matt Ingenthron <matt@couchbase.com>
 */
public class ObserveTester implements Runnable {

  CouchbaseClient cbc = null;
  final int times;
  private int getSuccess, getFailure, setSuccess, setFailure;
  private long getElapsedTime, setElapsedTime;

  public ObserveTester(CouchbaseClient client, int iterations) {
    cbc = client;
    times = iterations;

  }

  @Override
  public void run() {
    String threadName = Thread.currentThread().getName();
    getSuccess = 0;
    getFailure = 0;
    setSuccess = 0;
    setFailure = 0;
    getElapsedTime = 0;
    setElapsedTime = 0;

    Logger.getLogger(ObserveTester.class.getName()).log(Level.INFO,
            "Thread {0} starting.", threadName);
    for (int i = 0; i < times; i++) {
      try {

        if (i%2 == 0) {
          OperationFuture<Boolean> setRes = cbc.set(threadName + i, 0, "bar");
          if (!setRes.get()) {
            Logger.getLogger(ObserveTester.class.getName()).log(Level.SEVERE,
                    "Failed set on {0}.", new Object[]{threadName});
          }
        } else {
          cbc.get(threadName + i);
        }

        if (i % 100 == 0) {

          long preobserve = System.nanoTime();
          OperationFuture<Boolean> setResult = cbc.set("foo", 0, "bar", PersistTo.MASTER);
          if (!setResult.get()) {
            OperationStatus resStatus = setResult.getStatus();
            Logger.getLogger(ObserveTester.class.getName()).log(Level.SEVERE,
                    "Failed set on {0} for reason {1}.", new Object[]{threadName, resStatus.getMessage()});
            return;
          }
          long postobserve = System.nanoTime();
          long connectTime = postobserve - preobserve;
          Logger.getLogger(ObserveTester.class.getName()).log(Level.INFO,
                  "Did a set cycle in " + (double) connectTime / 1000000.0 + "ms.");
        }
        int sleeptime = getRandomNumberFrom(50, 200);
        Thread.sleep(sleeptime);
      } catch (InterruptedException ex) {
        Logger.getLogger(ObserveTester.class.getName()).log(Level.WARNING,
                "Thread waiting to do more terminated.", ex);
      } catch (ExecutionException ex) {
        Logger.getLogger(ObserveTester.class.getName()).log(Level.SEVERE,
                "Thread trying to work hit a problem, trying to continue.", ex);
      } catch (Exception e) {
        Logger.getLogger(ObserveTester.class.getName()).log(Level.WARNING,
                "Unexpected Exception in the main run() for loop", e);
        getFailure++;
      }
    }

    String wrongGetLatency = String.format("%1$,.2f", (getElapsedTime / 1000000.0) / (getSuccess + getFailure));
    String wrongSetLatency = String.format("%1$,.2f", (setElapsedTime / 1000000.0) / (setSuccess + setFailure));

    Logger.getLogger(ObserveTester.class.getName()).log(Level.INFO,
            "Thread {0} completed all of it's sets and gets. "
            + "Get successes: " + getSuccess + "; get failures: " + getFailure
            + " Set successes: " + setSuccess + "; set failures: " + setFailure
            + " Get rough latency average: " + wrongGetLatency + "ms"
            + " Set rough latency average: " + wrongSetLatency + "ms",
            threadName);
  }

  private void runSets(String threadName) throws InterruptedException {
    ArrayList<Future<Boolean>> gotten = new ArrayList<Future<Boolean>>();
    long preSetBomb = System.nanoTime();
    for (int i = 0; i < 1000; i++) {
      gotten.add(cbc.set(threadName + i, 0, threadName + i));
    }

    for (Future<Boolean> result : gotten) {
      try {
        if (result.get()) {
          setSuccess++;
        } else {
          setFailure++;
        }
      } catch (ExecutionException e) {
        // don't stop the thread, just log and move on
        Logger.getLogger(ObserveTester.class.getName()).log(Level.WARNING,
                "RuntimeException while checking sets in the future", e);
        setFailure++;
      } catch (Exception e) {
        Logger.getLogger(ObserveTester.class.getName()).log(Level.WARNING,
                "Unexpected Exception while checking gets in the future", e);
        getFailure++;
      }


    }
    long postSetVerify = System.nanoTime();
    setElapsedTime += postSetVerify - preSetBomb;
  }

  private void runGets(String threadName) throws InterruptedException,
          ExecutionException {
    // do some gets and check on the results
    ArrayList<Future<Object>> gotten = new ArrayList<Future<Object>>();
    long preGetBomb = System.nanoTime();
    for (int i = 0; i < 100; i++) {
      try {
        gotten.add(cbc.asyncGet(threadName + i));
      } catch (Exception e) {
        Logger.getLogger(ObserveTester.class.getName()).log(Level.WARNING,
                "RuntimeException while building get futures to check later",
                e);
      }
    }

    for (Future<Object> result : gotten) {
      try {
        result.get();
        getSuccess++;
      } catch (ExecutionException e) {
        // don't stop the thread, just log and move on
        Logger.getLogger(ObserveTester.class.getName()).log(Level.WARNING,
                "RuntimeException while checking gets in the future", e);
        getFailure++;
      } catch (Exception e) {
        Logger.getLogger(ObserveTester.class.getName()).log(Level.WARNING,
                "Unexpected Exception while checking gets in the future", e);
        getFailure++;
      }

    }
    long postGetVerify = System.nanoTime();
    getElapsedTime += postGetVerify - preGetBomb;

  }


  public static int getRandomNumberFrom(int min, int max) {
        Random foo = new Random();
        int randomNumber = foo.nextInt((max + 1) - min) + min;

        return randomNumber;

    }


}
