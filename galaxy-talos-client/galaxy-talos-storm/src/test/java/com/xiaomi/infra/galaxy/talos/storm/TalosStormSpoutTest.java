package com.xiaomi.infra.galaxy.talos.storm;

import backtype.storm.spout.SpoutOutputCollector;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.xiaomi.infra.galaxy.talos.consumer.MessageCheckpointer;
import com.xiaomi.infra.galaxy.talos.storm.config.TalosStormConfig;
import com.xiaomi.infra.galaxy.talos.storm.message.PartitionMessagePool;
import com.xiaomi.infra.galaxy.talos.storm.message.TalosStormMessage;
import com.xiaomi.infra.galaxy.talos.storm.message.TalosStormMessageProcessor;
import com.xiaomi.infra.galaxy.talos.thrift.Message;
import com.xiaomi.infra.galaxy.talos.thrift.MessageAndOffset;
import com.xiaomi.infra.galaxy.talos.thrift.TopicAndPartition;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import static org.mockito.Mockito.*;
import static org.junit.Assert.*;

/**
 * Created by jiasheng on 16-6-7.
 */
public class TalosStormSpoutTest {
    @Mock
    private SpoutOutputCollector collector;
    @Mock
    private TalosStormMessageProcessor msgProcessor;
    @Mock
    private PartitionMessagePool msgPool;
    @Mock
    private MessageCheckpointer msgCkp;

    private ConcurrentHashMap<String, TalosStormMessageProcessor> msgProcessors;
    private TalosStormSpout talosStormSpout;
    private static final String topic = "test-topic";
    private static final int partition = 0;
    private static final String message = "test-message";
    private static final long offset = 123l;
    private static final long commitInterval = 1000l;

    @Before
    public void prepare() throws Exception {
        MockitoAnnotations.initMocks(this);
        talosStormSpout = new TalosStormSpout(new TalosStormConfig(topic, "", null, null));
        msgProcessors = new ConcurrentHashMap<String, TalosStormMessageProcessor>();
        msgProcessors.put(Joiner.on("-").join(topic, partition), msgProcessor);

        prepareTalosStormSpout();
        prepareMessageProcessor();
        prepareMessagePool();
        prepareMessageCheckpointer();
        prepareOutputCollector();
    }

    private void prepareTalosStormSpout() throws NoSuchFieldException, IllegalAccessException {
        Field collectorField = TalosStormSpout.class.getDeclaredField("collector");
        collectorField.setAccessible(true);
        collectorField.set(talosStormSpout, collector);

        Field processorsField = TalosStormSpout.class.getDeclaredField("msgProcessors");
        processorsField.setAccessible(true);
        processorsField.set(talosStormSpout, msgProcessors);

        Field commitIntervalField = TalosStormSpout.class.getDeclaredField("commitIntervalMs");
        commitIntervalField.setAccessible(true);
        commitIntervalField.set(talosStormSpout, commitInterval);
    }

    private void prepareMessageProcessor() {
        when(msgProcessor.getMessagePool()).thenReturn(Optional.of(msgPool));
        when(msgProcessor.getMessageCheckpointer()).thenReturn(Optional.of(msgCkp));
        when(msgProcessor.getTopicPartition()).thenReturn(Optional.of(new TopicAndPartition(topic, null, partition)));
    }

    private void prepareMessagePool() {
        when(msgPool.get()).thenReturn(Optional.of(new TalosStormMessage(
                new TopicAndPartition(topic, null, partition),
                new MessageAndOffset(new Message(ByteBuffer.wrap(message.getBytes())), offset)
        )));
        when(msgPool.getCommitOffset()).thenReturn(Optional.of(offset));
    }

    private void prepareMessageCheckpointer() {
        when(msgCkp.checkpoint(anyLong())).thenReturn(true);
    }

    private void prepareOutputCollector() {
        when(collector.emit(anyList(), anyString())).thenReturn(null);
    }

    @Test
    public void testEmitAndCommit() throws InterruptedException {
        talosStormSpout.nextTuple();
        Thread.sleep(commitInterval);
        talosStormSpout.nextTuple();

        ArgumentCaptor<List> tupleCaptor = ArgumentCaptor.forClass(List.class);
        verify(collector, atLeastOnce()).emit(tupleCaptor.capture(), anyString());
        List tuple = tupleCaptor.getValue();
        assertEquals(message, tuple.get(0));
        assertEquals(offset, tuple.get(1));
        assertEquals(topic, tuple.get(2));
        assertEquals(partition, tuple.get(3));

        ArgumentCaptor<Long> offsetCaptor = ArgumentCaptor.forClass(Long.class);
        verify(msgCkp).checkpoint(offsetCaptor.capture());
        assertEquals(offset, offsetCaptor.getValue().longValue());
    }

}
