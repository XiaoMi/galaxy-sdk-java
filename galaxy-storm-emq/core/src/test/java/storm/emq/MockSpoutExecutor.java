package storm.emq;

import backtype.storm.Config;
import backtype.storm.spout.ISpout;
import backtype.storm.spout.ISpoutOutputCollector;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.topology.base.BaseRichSpout;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by jiasheng on 15-12-30.
 */
public class MockSpoutExecutor {
    private final ISpout spout;
    private final AtomicLong emitCount = new AtomicLong();
    private final AtomicLong ackCount = new AtomicLong();
    private final AtomicLong failCount = new AtomicLong();
    private final Random random = new Random();
    private volatile boolean running = true;

    public MockSpoutExecutor(ISpout spout) {
        this.spout = spout;
    }

    public void exec() {
        Map conf = new HashMap();
        final LinkedBlockingQueue<String> emitedMessages = new LinkedBlockingQueue<String>();
        conf.put(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS, 20);
        spout.open(conf, null, new SpoutOutputCollector(new ISpoutOutputCollector() {
            @Override
            public List<Integer> emit(String s, List<Object> list, Object o) {
                try {
                    emitedMessages.put(o.toString());
                    emitCount.incrementAndGet();
                } catch (InterruptedException e) {
                }
                return null;
            }

            @Override
            public void emitDirect(int i, String s, List<Object> list, Object o) {

            }

            @Override
            public void reportError(Throwable throwable) {

            }
        }));
        //do work
        while (running) {
            spout.nextTuple();
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
            }
            if (emitedMessages.isEmpty() && emitCount.get() != 0)
                this.stop();
            String message = null;
            while ((message = emitedMessages.poll()) != null) {
                if (random.nextInt(100) > 95) {
                    spout.fail(message);
                    failCount.incrementAndGet();
                } else {
                    spout.ack(message);
                    ackCount.incrementAndGet();
                }
            }
        }
    }

    public long getEmitCount() {
        return emitCount.get();
    }

    public long getAckCount() {
        return ackCount.get();
    }

    public long getFailCount() {
        return failCount.get();
    }

    public void stop() {
        spout.close();
        running = false;
    }
}
