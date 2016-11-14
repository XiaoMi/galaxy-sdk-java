/**
 * Copyright 2016, Xiaomi.
 * All rights reserved.
 * Author: xiajun@xiaomi.com
 */

package com.xiaomi.infra.galaxy.lcs.log.log4j;

public class TestLCSLogger {
  public static void main(String[] args) throws Exception {
    LCSLogger talosLogger = new LCSLogger();

    long messageNumber = 0;

    while (true) {
      talosLogger.write(("This is a test message for talos data flow, there is " +
          "nothing is it.This is a test message for talos data flow, " +
          "there inothing is it.This is a test message for talos data flow, " +
          "there inothing is it.This is a test message for talos data flow, " +
          "there inothing is it.This is a test message for talos data flow, " +
          "there inothing is it.This is a test message for talos data flow, " +
          "there inothing is it.This is a test message for talos data flow, " +
          "there inothing is it.This is a test message for talos data flow, " +
          "there inothing is it.This is a test message for talos data flow, " +
          "there inothing is it.This is a test message for talos data flow").getBytes());
      messageNumber ++;

      if (messageNumber % 1000 == 0) {
        System.out.println("MessageNumber: " + messageNumber);
        try {
          Thread.sleep(200);
        } catch (InterruptedException e) {

        }
      }

      if (messageNumber == 30000000) {
        break;
      }
    }

  }

}
