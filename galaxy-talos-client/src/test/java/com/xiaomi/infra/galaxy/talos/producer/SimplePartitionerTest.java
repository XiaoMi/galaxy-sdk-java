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
    assertEquals(1, partitioner.partition("1", 8));
    assertEquals(2, partitioner.partition("2", 8));
    assertEquals(3, partitioner.partition("3", 8));
    assertEquals(4, partitioner.partition("4", 8));
    assertEquals(5, partitioner.partition("5", 8));
    assertEquals(6, partitioner.partition("6", 8));
    assertEquals(7, partitioner.partition("7", 8));
  }
}
