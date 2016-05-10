/**
 * Copyright 2015, Xiaomi.
 * All rights reserved.
 * Author: yongxing@xiaomi.com
 */

package com.xiaomi.infra.galaxy.talos.consumer;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.xiaomi.infra.galaxy.talos.thrift.ConsumerService;
import com.xiaomi.infra.galaxy.talos.thrift.ErrorCode;
import com.xiaomi.infra.galaxy.talos.thrift.GalaxyTalosException;
import com.xiaomi.infra.galaxy.talos.thrift.LockPartitionRequest;
import com.xiaomi.infra.galaxy.talos.thrift.LockPartitionResponse;
import com.xiaomi.infra.galaxy.talos.thrift.Message;
import com.xiaomi.infra.galaxy.talos.thrift.MessageAndOffset;
import com.xiaomi.infra.galaxy.talos.thrift.QueryOffsetRequest;
import com.xiaomi.infra.galaxy.talos.thrift.QueryOffsetResponse;
import com.xiaomi.infra.galaxy.talos.thrift.TopicTalosResourceName;
import com.xiaomi.infra.galaxy.talos.thrift.UnlockPartitionRequest;
import com.xiaomi.infra.galaxy.talos.thrift.UpdateOffsetRequest;
import com.xiaomi.infra.galaxy.talos.thrift.UpdateOffsetResponse;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.when;

public class PartitionFetcherTest {
  private static final Logger LOG = LoggerFactory.getLogger(PartitionFetcherTest.class);
  private static final String topicName = "MyTopic";
  private static final String resourceName = "12345#MyTopic#34595fkdiso456i390";
  private static final int partitionId = 7;
  private static final String consumerGroup = "MyConsumerGroup";
  private static final String workerId = "workerId";

  private static List<MessageAndOffset> messageAndOffsetList;
  private static List<MessageAndOffset> messageAndOffsetList2;

  private static ConsumerService.Iface consumerClientMock;
  private static SimpleConsumer simpleConsumerMock;
  private static MessageReader messageReaderMock;

  private static PartitionFetcher partitionFetcher;

  @Before
  public void setUp() throws Exception {
    messageAndOffsetList = new ArrayList<MessageAndOffset>();
    messageAndOffsetList2 = new ArrayList<MessageAndOffset>();

    MessageAndOffset messageAndOffset1 = new MessageAndOffset(new Message(
        ByteBuffer.wrap("message1".getBytes())), 1);
    MessageAndOffset messageAndOffset2 = new MessageAndOffset(new Message(
        ByteBuffer.wrap("message2".getBytes())), 2);
    MessageAndOffset messageAndOffset3 = new MessageAndOffset(new Message(
        ByteBuffer.wrap("message3".getBytes())), 3);
    messageAndOffsetList.add(messageAndOffset1);
    messageAndOffsetList.add(messageAndOffset2);
    messageAndOffsetList.add(messageAndOffset3);
    MessageAndOffset messageAndOffset4 = new MessageAndOffset(new Message(
        ByteBuffer.wrap("message4".getBytes())), 4);
    MessageAndOffset messageAndOffset5 = new MessageAndOffset(new Message(
        ByteBuffer.wrap("message5".getBytes())), 5);
    messageAndOffsetList2.add(messageAndOffset4);
    messageAndOffsetList2.add(messageAndOffset5);

    consumerClientMock = Mockito.mock(ConsumerService.Iface.class);
    simpleConsumerMock = Mockito.mock(SimpleConsumer.class);
    messageReaderMock = Mockito.mock(MessageReader.class);

    partitionFetcher = new PartitionFetcher(consumerGroup, topicName,
        new TopicTalosResourceName(resourceName), partitionId, workerId,
        consumerClientMock, simpleConsumerMock, messageReaderMock);

    // mock value for simpleConsumerMock and consumerClientMock
    when(simpleConsumerMock.fetchMessage(anyLong()))
        .thenReturn(messageAndOffsetList).thenReturn(messageAndOffsetList2);
    UpdateOffsetResponse updateOffsetResponse = new UpdateOffsetResponse(true);
    when(consumerClientMock.updateOffset(any(UpdateOffsetRequest.class)))
        .thenReturn(updateOffsetResponse);
    doNothing().when(consumerClientMock)
        .unlockPartition(any(UnlockPartitionRequest.class));
    List<Integer> successPartitionList = new ArrayList<Integer>();
    successPartitionList.add(partitionId);
    LockPartitionResponse lockPartitionResponse = new LockPartitionResponse(
        successPartitionList, new ArrayList<Integer>());
    when(consumerClientMock.lockPartition(any(LockPartitionRequest.class)))
        .thenReturn(lockPartitionResponse);
    QueryOffsetResponse queryOffsetResponse = new QueryOffsetResponse(0);
    when(consumerClientMock.queryOffset(any(QueryOffsetRequest.class)))
        .thenReturn(queryOffsetResponse);
  }

  @After
  public void tearDown() {
    partitionFetcher.shutDown();
  }

  @Test
  public void testLockFailedFromLockedToUnlocked() throws Exception {
    doThrow(new GalaxyTalosException()).when(consumerClientMock)
        .lockPartition(any(LockPartitionRequest.class));

    partitionFetcher.lock();
    // sleep to wait stealing lock failed
    Thread.sleep(50);
    assertEquals(false, partitionFetcher.isServing());
  }

  @Test
  public void testLockFailedWhenNullSuccessList() throws Exception {
    List<Integer> successPartitionList = new ArrayList<Integer>();
    LockPartitionResponse lockPartitionResponse = new LockPartitionResponse(
        successPartitionList, new ArrayList<Integer>());
    when(consumerClientMock.lockPartition(any(LockPartitionRequest.class)))
        .thenReturn(lockPartitionResponse);

    partitionFetcher.lock();
    // sleep to wait lock
    Thread.sleep(50);
    assertEquals(false, partitionFetcher.isServing());
  }

  @Test
  public void testLockFailedWhenGetStartOffset() throws Exception {
    doThrow(new GalaxyTalosException()).when(messageReaderMock)
        .initStartOffset();

    partitionFetcher.lock();
    Thread.sleep(50);
    assertEquals(false, partitionFetcher.isServing());
  }

  @Test
  public void testGetMessageAndCommitOffset() throws Exception {
    partitionFetcher.lock();
    assertEquals(true, partitionFetcher.isHoldingLock());
    Thread.sleep(100);
    assertEquals(true, partitionFetcher.isHoldingLock());
    partitionFetcher.unlock();
    assertEquals(true, partitionFetcher.isHoldingLock());
    Thread.sleep(50);
    assertEquals(false, partitionFetcher.isServing());
  }

  // check log for: 1) call message 2/3 times;
  // 2) commit offset 5 even if getMultiple messages
  @Test
  public void testCallGetMessageMultipleTimesAndCommitOffset() throws Exception {
    partitionFetcher.lock();
    assertEquals(true, partitionFetcher.isHoldingLock());
    Thread.sleep(500);
    assertEquals(true, partitionFetcher.isHoldingLock());
    partitionFetcher.unlock();
  }

  @Test
  public void testPartitionNotServing() throws Exception {
    when(simpleConsumerMock.fetchMessage(anyLong()))
        .thenThrow(new GalaxyTalosException().setErrorCode(
            ErrorCode.PARTITION_NOT_SERVING));
    partitionFetcher.lock();
    Thread.sleep(500);
  }

  @Test
  public void testOffsetOutOfRange() throws Exception {
    when(simpleConsumerMock.fetchMessage(anyLong()))
        .thenThrow(new GalaxyTalosException().setErrorCode(
            ErrorCode.MESSAGE_OFFSET_OUT_OF_RANGE));
    partitionFetcher.lock();
    Thread.sleep(500);
  }

  @Test
  public void testLockLock() throws Exception {
    partitionFetcher.lock();
    Thread.sleep(50);
    partitionFetcher.lock();
    Thread.sleep(50);
  }

  @Test
  public void testUnlockUnlock() throws Exception {
    partitionFetcher.lock();
    partitionFetcher.unlock();
    Thread.sleep(50);
    partitionFetcher.unlock();
  }

  @Test
  public void testUnlockLock() throws Exception {
    partitionFetcher.lock();
    partitionFetcher.unlock();
    partitionFetcher.lock();
  }
}