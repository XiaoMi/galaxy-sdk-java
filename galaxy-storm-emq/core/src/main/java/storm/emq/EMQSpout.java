package storm.emq;

import backtype.storm.Config;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.utils.Utils;
import com.xiaomi.infra.galaxy.emq.client.EMQClientFactory;
import com.xiaomi.infra.galaxy.emq.thrift.*;
import libthrift091.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Created by jiasheng on 15-12-23.
 */
public class EMQSpout extends BaseRichSpout {
    public static final Logger LOG = LoggerFactory.getLogger(EMQSpout.class);
    final EMQConfig emqConfig;

    DeleteMessageThread deleteMessageThread;
    FetchMessageThread fetchMessageThread;

    LinkedBlockingQueue<String> ackedMessagesQueue;
    LinkedBlockingQueue<ReceiveMessageResponse> fetchedMessageQueue;

    SpoutOutputCollector collector;
    MessageService.Iface messageClient;


    public EMQSpout(EMQConfig emqConfig) {
        this.emqConfig = emqConfig;
    }


    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(emqConfig.emqScheme.getOutputFields());
    }


    private void checkTopologyTimeout(QueueService.Iface queueClient, Map map) {
        int topologyTimeout = Utils.getInt(map.get(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS));
        GetQueueInfoRequest getQueueInfoRequest = new GetQueueInfoRequest(emqConfig.queueName);
        GetQueueInfoResponse response = null;
        try {
            response = queueClient.getQueueInfo(getQueueInfoRequest);
        } catch (TException e) {
            throw new RuntimeException("Get EMQ queue info failed: " + e);
        }

        int emqInvisibleTime = response.getQueueAttribute().getInvisibilitySeconds();
        if (emqInvisibleTime < topologyTimeout)
            throw new RuntimeException("TOPOLOGY_MESSAGE_TIMEOUT_SECS(" + topologyTimeout +
                    "s) must small than EMQ queue invisibilitySeconds(" + emqInvisibleTime + "s)");
    }


    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        collector = spoutOutputCollector;
        EMQClientFactory clientFactory = new EMQClientFactory(emqConfig.credential);
        messageClient = clientFactory.newMessageClient(emqConfig.endpoint);

        checkTopologyTimeout(clientFactory.newQueueClient(emqConfig.endpoint), map);

        deleteMessageThread = new DeleteMessageThread();
        fetchMessageThread = new FetchMessageThread();
        ackedMessagesQueue = new LinkedBlockingQueue<String>();
        int capacity = emqConfig.emqCoordinator.newReceiveMessageRequest().getMaxReceiveMessageNumber();
        fetchedMessageQueue = new LinkedBlockingQueue<ReceiveMessageResponse>(capacity);

        fetchMessageThread.start();
        deleteMessageThread.start();
        LOG.info("Open EMQSpout");
    }

    @Override
    public void close() {
        fetchMessageThread.stopRunning();
        deleteMessageThread.stopRunning();
        LOG.info("Close EMQSpout");
    }

    @Override
    public void nextTuple() {
        try {
            ReceiveMessageResponse response = fetchedMessageQueue.poll(emqConfig.generateTupleTimeoutMs, TimeUnit.MILLISECONDS);
            if (response != null)
                collector.emit(emqConfig.emqScheme.deserialize(response), response.getReceiptHandle());
        } catch (InterruptedException e) {
            LOG.warn("Poll message from queue to generate tuple failed:" + e);
        }
    }


    @Override
    public void ack(Object msgId) {
        try {
            ackedMessagesQueue.put(msgId.toString());
        } catch (InterruptedException e) {
            LOG.warn("Ack message failed: " + e);
        }
    }


    @Override
    public void fail(Object msgId) {
        LOG.warn("Fail message with ReceiptHandle: " + msgId);
    }

    private class FetchMessageThread extends Thread {
        private volatile boolean running = true;

        public FetchMessageThread() {
            super("EMQSpout-fetch-messages-thread");
        }

        @Override
        public void run() {
            while (running) {
                try {
                    List<ReceiveMessageResponse> messageResponseList = messageClient.receiveMessage(
                            new ReceiveMessageRequest(emqConfig.emqCoordinator.newReceiveMessageRequest()));
                    if (messageResponseList != null && !messageResponseList.isEmpty()) {
                        for (ReceiveMessageResponse response : messageResponseList) {
                            fetchedMessageQueue.put(response);
                        }
                    }
                } catch (Exception e) {
                    LOG.warn("Fetch messages exception: " + e);
                }
            }
        }

        public void stopRunning() {
            running = false;
        }
    }

    private class DeleteMessageThread extends Thread {
        private long lastSend = System.currentTimeMillis();
        private volatile boolean running = true;

        public DeleteMessageThread() {
            super("EMQSpout-delete-messages-thread");
        }

        @Override
        public void run() {
            while (running) {
                try {
                    DeleteMessageBatchRequest deleteRequest = new DeleteMessageBatchRequest();
                    deleteRequest.setQueueName(emqConfig.queueName);
                    int count = 0;

                    while (System.currentTimeMillis() - lastSend <= emqConfig.deleteMessageMaxDelayMs && count <= emqConfig.deleteMessageMaxNumPerBatch) {
                        deleteRequest.addToDeleteMessageBatchRequestEntryList(new DeleteMessageBatchRequestEntry(ackedMessagesQueue.take()));
                        count++;
                    }
                    if (count > 0)
                        messageClient.deleteMessageBatch(deleteRequest);
                    lastSend = System.currentTimeMillis();
                } catch (Exception e) {
                    LOG.warn("Delete messages failed: " + e);
                }
            }
        }

        public void stopRunning() {
            running = false;
        }
    }
}
