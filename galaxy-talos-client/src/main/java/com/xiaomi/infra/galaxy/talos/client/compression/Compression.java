/**
 * Copyright 2015, Xiaomi.
 * All rights reserved.
 * Author: xiajun@xiaomi.com
 */

package com.xiaomi.infra.galaxy.talos.client.compression;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.xiaomi.infra.galaxy.talos.thrift.Message;
import com.xiaomi.infra.galaxy.talos.thrift.MessageAndOffset;
import com.xiaomi.infra.galaxy.talos.thrift.MessageBlock;
import com.xiaomi.infra.galaxy.talos.thrift.MessageCompressionType;

public class Compression {
  private static final Logger LOG = LoggerFactory.getLogger(Compression.class);
  private static final Charset CHARSET = Charset.forName("UTF-8");

  public static MessageBlock compress(List<Message> messageList,
      MessageCompressionType compressionType) throws IOException {
    MessageBlock messageBlock = new MessageBlock();
    messageBlock.setCompressionType(compressionType);
    messageBlock.setMessageNumber(messageList.size());

    int size = getMessageListSize(messageList);
    try {
      ByteArrayOutputStream outputStream = new ByteArrayOutputStream(size);
      DataOutputStream dataOutputStream = CompressionFactory.
          getConpressedOutputStream(compressionType, outputStream);
      for (Message message : messageList) {
        // write sequence number;
        if (message.isSetSequenceNumber()) {
          byte[] sequenceNumberBytes = message.getSequenceNumber().getBytes(CHARSET);
          dataOutputStream.writeInt(sequenceNumberBytes.length);
          dataOutputStream.write(sequenceNumberBytes);
        } else {
          dataOutputStream.writeInt(0);
        }

        // write data;
        dataOutputStream.writeInt(message.getMessage().length);
        dataOutputStream.write(message.getMessage());

      }

      // close dataOutputStream;
      dataOutputStream.close();

      byte[] messageBlockData = outputStream.toByteArray();
      messageBlock.setMessageBlock(messageBlockData);
      messageBlock.setMessageBlockSize(messageBlockData.length);
    } catch (IOException e) {
      LOG.info("compress MessageList failed", e);
      throw e;
    }


    return messageBlock;
  }

  public static List<MessageAndOffset> decompress(List<MessageBlock> messageBlockList) throws IOException {
    List<MessageAndOffset> messageAndOffsetList = new ArrayList<MessageAndOffset>();
    for (MessageBlock messageBlock : messageBlockList) {
      List<MessageAndOffset> list = decompress(messageBlock);
      messageAndOffsetList.addAll(list);
    }

    return messageAndOffsetList;
  }

  public static List<MessageAndOffset> decompress(MessageBlock messageBlock) throws IOException {
    DataInputStream dataInputStream = CompressionFactory.getDeconpressedInputStream(
        messageBlock.getCompressionType(), ByteBuffer.wrap(messageBlock.getMessageBlock()));

    List<MessageAndOffset> messageAndOffsetList =
        new ArrayList<MessageAndOffset>(messageBlock.getMessageNumber());
    try {
      for (int i = 0; i < messageBlock.getMessageNumber(); ++i) {
        MessageAndOffset messageAndOffset = new MessageAndOffset();
        messageAndOffset.setMessageOffset(messageBlock.getStartMessageOffset() + i);
        Message message = new Message();
        messageAndOffset.setMessage(message);

        // read sequence number;
        int sequenceNumberSize = dataInputStream.readInt();
        if (sequenceNumberSize != 0) {
          byte[] sequenceNumberBytes = new byte[sequenceNumberSize];
          dataInputStream.readFully(sequenceNumberBytes);
          message.setSequenceNumber(new String(sequenceNumberBytes, CHARSET));
        }

        // read message data;
        int messageSize = dataInputStream.readInt();
        Preconditions.checkArgument(messageSize != 0);
        byte[] messageData = new byte[messageSize];
        dataInputStream.readFully(messageData);
        message.setMessage(messageData);

        // add message to messageList;
        messageAndOffsetList.add(messageAndOffset);
      }
    } catch (EOFException e) {
      LOG.error("Decompress messageBlock failed", e);
      Preconditions.checkArgument(false, "Decompress messageBlock failed, " + e);
    } catch (IOException e) {
      LOG.error("Decompress messageBlock failed", e);
    }

    return messageAndOffsetList;
  }

  private static int getMessageListSize(List<Message> messageList) {
    int size = 0;
    for (Message message : messageList) {
      size += 8;
      if (message.isSetSequenceNumber()) {
        size += message.getSequenceNumber().length();
      }

      size += message.getMessage().length;
    }

    return size;
  }
}
