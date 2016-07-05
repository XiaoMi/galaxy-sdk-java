package com.xiaomi.infra.galaxy.talos.storm;

import com.google.common.base.Optional;
import com.xiaomi.infra.galaxy.talos.storm.message.PartitionMessagePool;
import com.xiaomi.infra.galaxy.talos.storm.message.TalosStormMessage;
import com.xiaomi.infra.galaxy.talos.thrift.MessageAndOffset;
import com.xiaomi.infra.galaxy.talos.thrift.TopicAndPartition;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.*;


/**
 * Created by jiasheng on 16-5-31.
 */
public class PartitionMessagePoolTest {
    private static Set<TalosStormMessage> input = new LinkedHashSet<TalosStormMessage>();
    private static int partition = 0;
    private static long testDataSize = 1000;
    private PartitionMessagePool partitionMessagePool;
    private AtomicBoolean addDataFinished;

    @BeforeClass
    public static void prepareData() {
        for (int offset = 0; offset < testDataSize; offset++) {
            input.add(new TalosStormMessage(new TopicAndPartition(TestTalosStormConfig.topic, null, partition),
                    new MessageAndOffset(null, offset)));
        }
    }

    @Before
    public void addData() {
        partitionMessagePool = new PartitionMessagePool(TestTalosStormConfig.getConfig());
        addDataFinished = new AtomicBoolean(false);
        new Thread(new Runnable() {
            @Override
            public void run() {
                for (TalosStormMessage m : input) {
                    partitionMessagePool.put(m);
                }
                addDataFinished.set(true);
            }
        }).start();
    }

    @Test
    public void testDataAndOffset() {
        Optional<TalosStormMessage> messageOptional;
        List<TalosStormMessage> output = new ArrayList<TalosStormMessage>();
        Random random = new Random();
        do {
            messageOptional = partitionMessagePool.get();
            if (messageOptional.isPresent()) {
                if (random.nextInt(100) > 90) {
                    partitionMessagePool.fail(messageOptional.get().message.messageOffset);
                } else {
                    output.add(messageOptional.get());
                    partitionMessagePool.ack(messageOptional.get().message.messageOffset);
                }
            }

        } while (messageOptional.isPresent() || !addDataFinished.get());
        assertEquals("Not return complete data set", input.size(), output.size());

        Optional<Long> commitOffset = partitionMessagePool.getCommitOffset();
        assertTrue("No commit offset info", commitOffset.isPresent());
        assertEquals("Incorrect commit offset: " + commitOffset.get(),
                testDataSize - 1, commitOffset.get().longValue());
    }

    @Test
    public void testFailOffset() {
        Optional<TalosStormMessage> messageOptional;
        do {
            messageOptional = partitionMessagePool.get();
        } while (messageOptional.isPresent() || !addDataFinished.get());
        for (TalosStormMessage m : input) {
            if (m.message.messageOffset != testDataSize / 4 && m.message.messageOffset != testDataSize / 3
                    && m.message.messageOffset != testDataSize / 2) {
                partitionMessagePool.ack(m.message.messageOffset);
            }
        }
        Optional<Long> commitOffset = partitionMessagePool.getCommitOffset();
        assertTrue("No commit offset info", commitOffset.isPresent());
        assertEquals("Incorrect commit offset: " + commitOffset.get(),
                testDataSize / 4 - 1, commitOffset.get().longValue());
    }

    @AfterClass
    public static void clear() {
        input.clear();
    }
}
