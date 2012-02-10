/*
 * Copyright (C) 2011 Couchbase, Inc.
 * All rights reserved.
 */
package com.couchbase.demo;

import com.couchbase.client.CouchbaseClient;
import java.util.ArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * SleepingTester is designed to sleep for a random period of time and
 * then issue various operations to the server, much like a real user's code
 * would when running in an app server.  During the sleepy time, a real
 * application would be sending HTML to a user, calling other databases and
 * web services, or filling the log file.
 *
 * There are many places here where we should have better error handling.
 *
 * @author Matt Ingenthron <matt@couchbase.com>
 */
public class SleepingTester implements Runnable {

	CouchbaseClient cbc = null;
	final int times;
	private int getSuccess, getFailure, setSuccess, setFailure;
        private long getElapsedTime, setElapsedTime;

	public SleepingTester(CouchbaseClient client, int iterations) {
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

		Logger.getLogger(SleepingTester.class.getName()).log(Level.INFO,
						"Thread {0} starting.", threadName);
		for (int i = 0; i < times; i++) {
			try {
				runSets(threadName);
				Thread.sleep(10);
				runGets(threadName);
				Thread.sleep(30);
			} catch (InterruptedException ex) {
				Logger.getLogger(SleepingTester.class.getName()).log(Level.WARNING,
								"Thread waiting to do more terminated.", ex);
			} catch (ExecutionException ex) {
				Logger.getLogger(SleepingTester.class.getName()).log(Level.SEVERE,
								"Thread trying to work hit a problem, trying to continue.", ex);
			} catch (Exception e) {
				Logger.getLogger(SleepingTester.class.getName()).log(Level.WARNING,
								"Unexpected Exception in the main run() for loop", e);
				getFailure++;
			}
		}

                String wrongGetLatency = String.format("%1$,.2f", (getElapsedTime/ 1000000.0)/(getSuccess+getFailure));
                String wrongSetLatency = String.format("%1$,.2f", (setElapsedTime/ 1000000.0)/(setSuccess+setFailure));

		Logger.getLogger(SleepingTester.class.getName()).log(Level.INFO,
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
				Logger.getLogger(SleepingTester.class.getName()).log(Level.WARNING,
								"RuntimeException while checking sets in the future", e);
				setFailure++;
			} catch (Exception e) {
				Logger.getLogger(SleepingTester.class.getName()).log(Level.WARNING,
								"Unexpected Exception while checking gets in the future", e);
				getFailure++;
			}


		}
                long postSetVerify = System.nanoTime();
                setElapsedTime += postSetVerify-preSetBomb;
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
				Logger.getLogger(SleepingTester.class.getName()).log(Level.WARNING,
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
				Logger.getLogger(SleepingTester.class.getName()).log(Level.WARNING,
								"RuntimeException while checking gets in the future", e);
				getFailure++;
			} catch (Exception e) {
				Logger.getLogger(SleepingTester.class.getName()).log(Level.WARNING,
								"Unexpected Exception while checking gets in the future", e);
				getFailure++;
			}

		}
                long postGetVerify = System.nanoTime();
                getElapsedTime += postGetVerify-preGetBomb;

	}
}
