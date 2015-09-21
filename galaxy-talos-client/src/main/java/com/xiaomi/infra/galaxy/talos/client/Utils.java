/**
 * Copyright 2015, Xiaomi.
 * All rights reserved.
 * Author: yongxing@xiaomi.com
 */

package com.xiaomi.infra.galaxy.talos.client;

import com.google.common.base.Preconditions;

import static com.xiaomi.infra.galaxy.talos.client.Constants.TALOS_IDENTIFIER_DELIMITER;

public class Utils {
  /**
   * The format of valid resource name is: ownerId#topicName#UUID
   * Note the 'ownerId' may contains the symbol '#',
   * so return topicName parsing from the tail of resourceName.
   *
   * @param topicTalosResourceName
   * @return
   */
  public static String getTopicNameByResourceName(String topicTalosResourceName) {
    String[] itemList = topicTalosResourceName.split(TALOS_IDENTIFIER_DELIMITER);
    Preconditions.checkArgument(itemList.length >= 3);
    return itemList[itemList.length - 2];
  }
}
