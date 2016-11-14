/**
 * Copyright 2016, Xiaomi.
 * All rights reserved.
 * Author: xiajun@xiaomi.com
 */

package com.xiaomi.infra.galaxy.lcs.log.log4j.appender;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.Enumeration;

import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.Layout;
import org.apache.log4j.Logger;
import org.apache.log4j.MDC;
import org.apache.log4j.spi.LoggingEvent;

import com.xiaomi.infra.galaxy.lcs.log.core.ILogger;
import com.xiaomi.infra.galaxy.lcs.log.core.appender.LCSAppender;
import com.xiaomi.infra.galaxy.lcs.log.core.utils.ConfigureChecker;
import com.xiaomi.infra.galaxy.talos.thrift.Message;
import com.xiaomi.infra.galaxy.talos.thrift.MessageType;

abstract public class LCSLog4jAppender extends AppenderSkeleton {
  private final ILogger logger;

  protected LCSAppender lcsAppender;

  private String topicName;
  private String encoding;
  private long maxBufferMessageBytes;
  private long maxBufferMessageNumber;
  private boolean blockWhenBufferFull;
  private long flushMessageBytes;
  private long flushMessageNumber;
  private long flushIntervalMillis;
  private long periodCheckIntervalMillis;
  private String localHostname;

  public LCSLog4jAppender(ILogger logger, LCSAppender lcsAppender) {
    this.logger = logger;
    this.lcsAppender = lcsAppender;

    this.encoding = "UTF-8";
    try {
      localHostname = getIp();
    } catch (Exception e) {
      localHostname = "127.0.0.1";
    }
  }

  public String getTopicName() {
    return topicName;
  }

  public void setTopicName(String topicName) {
    ConfigureChecker.checkConfigureBlank("topicName", topicName);
    this.topicName = topicName;
    lcsAppender.setTopicName(topicName);
  }

  public String getEncoding() {
    return encoding;
  }

  public void setEncoding(String encoding) {
    ConfigureChecker.checkConfigureBlank("encoding", encoding);
    this.encoding = encoding;
  }

  public long getMaxBufferMessageBytes() {
    return maxBufferMessageBytes;
  }

  public void setMaxBufferMessageBytes(long maxBufferMessageBytes) {
    ConfigureChecker.checkConfigureRange("maxBufferMessageBytes",
        maxBufferMessageBytes, 1024 * 1024, Long.MAX_VALUE);

    this.maxBufferMessageBytes = maxBufferMessageBytes;
    lcsAppender.setMaxBufferMessageBytes(maxBufferMessageBytes);
  }

  public long getMaxBufferMessageNumber() {
    return maxBufferMessageNumber;
  }

  public void setMaxBufferMessageNumber(long maxBufferMessageNumber) {
    ConfigureChecker.checkConfigureRange("maxBufferMessageNumber",
        maxBufferMessageNumber, 1000L, Long.MAX_VALUE);

    this.maxBufferMessageNumber = maxBufferMessageNumber;
    lcsAppender.setMaxBufferMessageNumber(maxBufferMessageNumber);
  }

  public boolean isBlockWhenBufferFull() {
    return blockWhenBufferFull;
  }

  public void setBlockWhenBufferFull(boolean blockWhenBufferFull) {
    this.blockWhenBufferFull = blockWhenBufferFull;
    lcsAppender.setBlockWhenBufferFull(blockWhenBufferFull);
  }

  public long getFlushMessageBytes() {
    return flushMessageBytes;
  }

  public void setFlushMessageBytes(long flushMessageBytes) {
    ConfigureChecker.checkConfigureRange("flushMessageBytes",
        flushMessageBytes, 1, 10 * 1024 * 1024);

    this.flushMessageBytes = flushMessageBytes;
    lcsAppender.setFlushMessageBytes(flushMessageBytes);
  }

  public long getFlushMessageNumber() {
    return flushMessageNumber;
  }

  public void setFlushMessageNumber(long flushMessageNumber) {
    ConfigureChecker.checkConfigureRange("flushMessageNumber",
        flushMessageNumber, 1, 50000L);

    this.flushMessageNumber = flushMessageNumber;
    lcsAppender.setFlushMessageNumber(flushMessageNumber);
  }

  public long getFlushIntervalMillis() {
    return flushIntervalMillis;
  }

  public void setFlushIntervalMillis(long flushIntervalMillis) {
    ConfigureChecker.checkConfigureRange("flushIntervalMillis",
        flushIntervalMillis, 1L, 10L * 60 * 1000);

    this.flushIntervalMillis = flushIntervalMillis;
    lcsAppender.setFlushIntervalMillis(flushIntervalMillis);
  }

  public long getPeriodCheckIntervalMillis() {
    return periodCheckIntervalMillis;
  }

  public void setPeriodCheckIntervalMillis(long periodCheckIntervalMillis) {
    ConfigureChecker.checkConfigureRange("periodCheckIntervalMillis",
        periodCheckIntervalMillis, 10L, 10L * 60 * 1000);

    this.periodCheckIntervalMillis = periodCheckIntervalMillis;
    lcsAppender.setPeriodCheckIntervalMillis(periodCheckIntervalMillis);
  }

  public String getLocalHostname() {
    return localHostname;
  }

  public void setLocalHostname(String localHostname) {
    this.localHostname = localHostname;
  }

  @Override
  protected void append(LoggingEvent loggingEvent) {
    Message message;
    if (loggingEvent.getMessage() instanceof Message) {
      message = (Message) loggingEvent.getMessage();
    } else {
      String value = layout.format(loggingEvent);
      if (layout.ignoresThrowable()) {
        String[] lineList = loggingEvent.getThrowableStrRep();
        if (lineList != null) {
          for (String line : lineList) {
            value += line + Layout.LINE_SEP;
          }
        }
      }

      message = new Message();
      message.setCreateTimestamp(System.currentTimeMillis());
      message.setMessageType(MessageType.BINARY);
      try {
        message.setMessage(value.getBytes(encoding));
      } catch (Exception e) {
        logger.error("appendMessage failed", e);
        return;
      }
    }
    lcsAppender.addMessage(message);
  }

  @Override
  public void activateOptions() {
    if (layout == null) {
      throw new RuntimeException("Please set layout for LCSFileAppender");
    }

    MDC.put("IP", localHostname);
    super.activateOptions();
    lcsAppender.start();
  }

  @Override
  public boolean requiresLayout() {
    return true;
  }

  @Override
  public void close() {
    lcsAppender.close();
  }

  /**
   * Copyed from scribe-log4j;
   *
   * 多IP处理，可以得到最终ip
   *
   * @return
   * @throws java.net.SocketException
   */
  private String getIp() throws SocketException {
    String localip = null;// 本地IP，如果没有配置外网IP则返回它
    String netip = null;// 外网IP

    Enumeration<NetworkInterface> netInterfaces = NetworkInterface.getNetworkInterfaces();
    InetAddress ip = null;
    boolean finded = false;// 是否找到外网IP
    while (netInterfaces.hasMoreElements() && !finded) {
      NetworkInterface ni = netInterfaces.nextElement();
      Enumeration<InetAddress> address = ni.getInetAddresses();
      while (address.hasMoreElements()) {
        ip = address.nextElement();
        if (!ip.isSiteLocalAddress() && !ip.isLoopbackAddress() && ip.getHostAddress().indexOf(":") == -1) {// 外网IP
          netip = ip.getHostAddress();
          finded = true;
          break;
        } else if (ip.isSiteLocalAddress() && !ip.isLoopbackAddress() && ip.getHostAddress().indexOf(":") == -1) {// 内网IP
          localip = ip.getHostAddress();
        }
      }
    }

    if (netip != null && !"".equals(netip)) {
      return netip;
    } else {
      return localip;
    }
  }

}
