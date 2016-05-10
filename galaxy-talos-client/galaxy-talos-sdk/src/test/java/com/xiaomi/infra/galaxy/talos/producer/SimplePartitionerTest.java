/**
 * Copyright 2015, Xiaomi.
 * All rights reserved.
 * Author: yongxing@xiaomi.com
 */

package com.xiaomi.infra.galaxy.talos.producer;

import java.util.UUID;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class SimplePartitionerTest {
  private static SimplePartitioner partitioner;

  @Before
  public void setUp() {
    partitioner = new SimplePartitioner();
  }

  // just test for 4 partition
  private int getPartitionId(int hashcode) {
    if (hashcode >= 0 && hashcode < 536870911) {
      return 0;
    } else if (hashcode >= 536870911 && hashcode < 1073741822) {
      return 1;
    } else if (hashcode >= 1073741822 && hashcode < 1610612733) {
      return 2;
    } else if (hashcode >= 1610612733 && hashcode < 2147483644) {
      return 3;
    }
    return 0;
  }

  private String randomKey() {
    return UUID.randomUUID().toString();
  }

  // test 70 times
  @Test
  public void testPartition() {
    for (int i = 0; i < 70; ++i) {
      String key = randomKey();
      assertEquals(getPartitionId(key.hashCode() & 0x7FFFFFFF),
          partitioner.partition(key, 4));
    }
  }
}
