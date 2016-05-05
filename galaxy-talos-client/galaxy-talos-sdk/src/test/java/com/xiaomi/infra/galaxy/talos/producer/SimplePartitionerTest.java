/**
 * Copyright 2015, Xiaomi.
 * All rights reserved.
 * Author: yongxing@xiaomi.com
 */

package com.xiaomi.infra.galaxy.talos.producer;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class SimplePartitionerTest {
  private static SimplePartitioner partitioner;

  @Before
  public void setUp() {
    partitioner = new SimplePartitioner();
  }

  @Test
  public void testPartition() {
    assertEquals(0, partitioner.partition("0", 8));
    assertEquals(0, partitioner.partition("1", 8));

    /*
    // test when partitioner return:
    // (Integer.parseInt(partitionKey) & 0x7FFFFFFF) / partitionInterval;
    assertEquals(0, partitioner.partition("268435454", 8));
    assertEquals(1, partitioner.partition("268435455", 8));
    */
  }
}
