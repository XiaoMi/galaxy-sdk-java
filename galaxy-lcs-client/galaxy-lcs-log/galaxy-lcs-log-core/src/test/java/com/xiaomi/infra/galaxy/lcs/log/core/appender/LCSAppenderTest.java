/**
 * Copyright 2016, Xiaomi.
 * All rights reserved.
 * Author: xiajun@xiaomi.com
 */

package com.xiaomi.infra.galaxy.lcs.log.core.appender;

import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.InOrder;
import org.mockito.Mockito;

import com.xiaomi.infra.galaxy.lcs.log.core.ILogger;
import com.xiaomi.infra.galaxy.lcs.log.core.dummy.DummyLogger;
import com.xiaomi.infra.galaxy.talos.client.serialization.MessageSerialization;
import com.xiaomi.infra.galaxy.talos.client.serialization.MessageSerializationFactory;
import com.xiaomi.infra.galaxy.talos.thrift.Message;
import com.xiaomi.infra.galaxy.talos.thrift.MessageType;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyList;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.times;

public class LCSAppenderTest {
  private interface TestHandler {
    public void doStart();
    public void doFlushMessage(List<Message> messageList) throws Exception;
    public void doPeriodCheck();
    public void doClose();
  }

  private class LCSAppenderImpl extends LCSAppender {
    private TestHandler testHandler;

    private LCSAppenderImpl(ILogger logger, TestHandler testHandler) {
      super(logger);

      this.testHandler = testHandler;
    }

    @Override
    protected void doStart() {
      testHandler.doStart();
    }

    @Override
    protected void doFlushMessage(List<Message> messageList) throws Exception {
      testHandler.doFlushMessage(messageList);
    }

    @Override
    protected void doPeriodCheck() {
      testHandler.doPeriodCheck();
    }

    @Override
    protected void doClose() {
      testHandler.doClose();
    }
  }

  private static String topicName;
  private static Message message;
  private static List<Message> messageList;

  private TestHandler testHandler;
  private LCSAppenderImpl lcsAppender;

  @BeforeClass
  public static void setUpTestCase() throws Exception {
    topicName = "LCSAppenderTestTopic";
    message = new Message();
    message.setCreateTimestamp(System.currentTimeMillis());
    message.setMessageType(MessageType.BINARY);
    message.setSequenceNumber("sequenceNumber");
    message.setMessage("message".getBytes());

    messageList = new ArrayList<Message>(1);
    messageList.add(message);

  }

  @Before
  public void setUp() throws Exception {
    testHandler = Mockito.mock(TestHandler.class);
    lcsAppender = new LCSAppenderImpl(new DummyLogger(), testHandler);
    lcsAppender.setTopicName(topicName);
  }

  @Test
  public void testFlushByMessageNumber() throws Exception {
    InOrder inOrder = inOrder(testHandler);
    lcsAppender.setFlushIntervalMillis(10000000);
    lcsAppender.setFlushMessageNumber(1);

    doNothing().when(testHandler).doStart();
    lcsAppender.start();
    inOrder.verify(testHandler, times(1)).doStart();

    doNothing().when(testHandler).doFlushMessage(anyList());
    lcsAppender.addMessage(message);
    // wait
    Thread.sleep(100);
    inOrder.verify(testHandler, times(1)).doFlushMessage(anyList());

    doNothing().when(testHandler).doFlushMessage(messageList);
    lcsAppender.addMessage(message);
    Thread.sleep(100);
    inOrder.verify(testHandler, times(1)).doFlushMessage(messageList);

    doNothing().when(testHandler).doClose();
    lcsAppender.close();
    inOrder.verify(testHandler, times(1)).doClose();

    inOrder.verifyNoMoreInteractions();
  }

  @Test
  public void testFlushByMessageBytes() throws Exception {
    InOrder inOrder = inOrder(testHandler);
    lcsAppender.setFlushIntervalMillis(10000000);
    lcsAppender.setFlushMessageBytes(MessageSerialization.getMessageSize(message,
        MessageSerializationFactory.getDefaultMessageVersion()));

    doNothing().when(testHandler).doStart();
    lcsAppender.start();
    inOrder.verify(testHandler, times(1)).doStart();

    doNothing().when(testHandler).doFlushMessage(messageList);
    lcsAppender.addMessage(message);
    Thread.sleep(100);
    inOrder.verify(testHandler, times(1)).doFlushMessage(messageList);

    doNothing().when(testHandler).doFlushMessage(messageList);
    lcsAppender.addMessage(message);
    Thread.sleep(100);
    inOrder.verify(testHandler, times(1)).doFlushMessage(messageList);

    doNothing().when(testHandler).doClose();
    lcsAppender.close();
    inOrder.verify(testHandler, times(1)).doClose();

    inOrder.verifyNoMoreInteractions();
  }

  @Test
  public void testFlushMessageByTime() throws Exception {
    InOrder inOrder = inOrder(testHandler);
    lcsAppender.setFlushIntervalMillis(100);

    doNothing().when(testHandler).doStart();
    lcsAppender.start();
    inOrder.verify(testHandler, times(1)).doStart();

    doNothing().when(testHandler).doFlushMessage(messageList);
    lcsAppender.addMessage(message);
    Thread.sleep(100 * 2);
    inOrder.verify(testHandler, times(1)).doFlushMessage(messageList);

    doNothing().when(testHandler).doFlushMessage(messageList);
    lcsAppender.addMessage(message);
    Thread.sleep(100 * 2);
    inOrder.verify(testHandler, times(1)).doFlushMessage(messageList);

    doNothing().when(testHandler).doClose();
    lcsAppender.close();
    inOrder.verify(testHandler, times(1)).doClose();

    inOrder.verifyNoMoreInteractions();
  }

  @Test
  public void testFlushMessageFailed() throws Exception {
    InOrder inOrder = inOrder(testHandler);
    lcsAppender.setFlushMessageNumber(1);
    lcsAppender.setFlushIntervalMillis(10000000);

    doNothing().when(testHandler).doStart();
    lcsAppender.start();
    inOrder.verify(testHandler, times(1)).doStart();

    doThrow(new Exception()).doNothing().when(testHandler).doFlushMessage(messageList);
    lcsAppender.addMessage(message);
    Thread.sleep(100);
    inOrder.verify(testHandler, times(2)).doFlushMessage(messageList);

    doNothing().when(testHandler).doFlushMessage(messageList);
    lcsAppender.addMessage(message);
    Thread.sleep(100);
    inOrder.verify(testHandler, times(1)).doFlushMessage(messageList);

    doNothing().when(testHandler).doClose();
    lcsAppender.close();
    inOrder.verify(testHandler, times(1)).doClose();

    inOrder.verifyNoMoreInteractions();
  }

  @Test
  public void testPeriodCheck() throws Exception {
    InOrder inOrder = inOrder(testHandler);
    lcsAppender.setFlushIntervalMillis(10000000);
    lcsAppender.setPeriodCheckIntervalMillis(200);

    doNothing().when(testHandler).doStart();
    lcsAppender.start();
    inOrder.verify(testHandler, times(1)).doStart();

    doNothing().when(testHandler).doPeriodCheck();
    Thread.sleep(200 + 50);
    inOrder.verify(testHandler).doPeriodCheck();

    doNothing().when(testHandler).doClose();
    lcsAppender.close();
    inOrder.verify(testHandler, times(1)).doClose();

    inOrder.verifyNoMoreInteractions();

  }
}
