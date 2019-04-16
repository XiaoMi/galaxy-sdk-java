/**
 * Copyright 2019, Xiaomi. All rights reserved. Author: zhangqian8@xiaomi.com
 */
package com.xiaomi.infra.codelab.talos;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.xiaomi.infra.galaxy.rpc.thrift.Credential;
import com.xiaomi.infra.galaxy.talos.producer.SimpleProducer;
import com.xiaomi.infra.galaxy.talos.producer.TalosProducerConfig;
import com.xiaomi.infra.galaxy.talos.thrift.Message;
import com.xiaomi.infra.galaxy.talos.thrift.TopicAndPartition;

public class PartitionSendStressor {
    private static final Logger LOG = LoggerFactory.getLogger(PartitionSendStressor.class);

    private SimpleProducer simpleProducer;
    private Map<Integer, ScheduledExecutorService> executorsMap = new HashMap<Integer, ScheduledExecutorService>();
    private int putInterval;  //ms
    private List<Message> messageList = new ArrayList<Message>();

    private static final AtomicLong successPutNumber = new AtomicLong(0);

    private class PutTask implements Runnable {
        @Override
        public void run() {
            try {
                simpleProducer.putMessageList(messageList);
                if (successPutNumber.addAndGet(1) % 100 == 0) {
                    LOG.info("success to put message: " + successPutNumber.get() + " so far.");
                }
            } catch (Exception e) {
                LOG.warn("put failed." + e);
            }
        }
    }

    public PartitionSendStressor(int partitionId, int threadNum, int qpsPerThread,
        TalosProducerConfig producerConfig, TopicAndPartition topicAndPartition, Message message, Credential credential) {
        simpleProducer = new SimpleProducer(producerConfig,
            topicAndPartition, credential);

        if (qpsPerThread <= 0) {
            putInterval = 1;
            LOG.info("the put qps/thread is at fast rate!");
        } else {
            putInterval = 1000/qpsPerThread;
            LOG.info("the put qps/thread setted is: " + qpsPerThread + ". And the real put qps/thread is: " + 1000/putInterval);
        }

        messageList.add(message);

        for (int thread = 0; thread < threadNum; thread++) {
            executorsMap.put(Integer.reverse(thread), Executors.newSingleThreadScheduledExecutor());
            executorsMap.get(Integer.reverse(thread)).scheduleAtFixedRate(new PutTask(), 0,
                putInterval, TimeUnit.MILLISECONDS);
        }
    }

}
