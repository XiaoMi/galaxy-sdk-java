package storm.emq;

import com.xiaomi.infra.galaxy.emq.client.EMQClientFactory;
import com.xiaomi.infra.galaxy.emq.thrift.*;
import com.xiaomi.infra.galaxy.rpc.thrift.Credential;
import com.xiaomi.infra.galaxy.rpc.thrift.UserType;
import libthrift091.TException;
import org.junit.*;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertTrue;

/**
 * Created by jiasheng on 15-12-25.
 */
public class EMQConfigTest {
    private static String queueName;

    private final String messageBody = "storm-emq-test-message-body";
    private EMQConfig emqConfig;
    private EMQCoordinator coordinator;
    private MessageService.Iface messageClient;


    private void initEMQConfig() {
        emqConfig = new EMQConfig(EMQTestConfig.EMQ_ENDPOINT, queueName, EMQTestConfig.EMQ_TAG, QueueUtil.getCredential());
        coordinator = emqConfig.emqCoordinator;
        messageClient = new EMQClientFactory(emqConfig.credential).newMessageClient(emqConfig.endpoint);
    }

    @BeforeClass
    public static void init() throws TException, InterruptedException {
        queueName = QueueUtil.createQueue();
    }

    @AfterClass
    public static void destroy() throws TException, InterruptedException {
        QueueUtil.deleteQueue(queueName);
    }


    @Before
    public void setUp() throws TException {
        initEMQConfig();
    }

    @After
    public void tearDown() {
    }

    private void sendMessage() throws TException {
        SendMessageRequest sendMessageRequest = new SendMessageRequest(queueName, messageBody);
        messageClient.sendMessage(sendMessageRequest);
    }

    private List<ReceiveMessageResponse> receiveMessage() throws TException {
        ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(coordinator.newReceiveMessageRequest());
        List<ReceiveMessageResponse> responses = null;
        while (responses == null || responses.isEmpty()) {
            responses = messageClient.receiveMessage(receiveMessageRequest);
        }
        return responses;
    }

    private void deleteMessage(List<ReceiveMessageResponse> responses) {
        DeleteMessageBatchRequest deleteMessageBatchRequest = new DeleteMessageBatchRequest();
        deleteMessageBatchRequest.setQueueName(queueName);
        for (ReceiveMessageResponse response : responses) {
            deleteMessageBatchRequest.addToDeleteMessageBatchRequestEntryList(
                    new DeleteMessageBatchRequestEntry(response.getReceiptHandle())
            );
        }
    }

    @Test
    public void testReceiveMessage() throws TException {
        sendMessage();
        List<ReceiveMessageResponse> responses = receiveMessage();

        Set<String> messages = new HashSet<String>();
        for (ReceiveMessageResponse response : responses) {
            messages.add(response.getMessageBody());
        }

        deleteMessage(responses);
        assertTrue(messages.contains(messageBody));
    }


    @Test
    public void testTag() throws TException {
        QueueUtil.createTag(queueName);
        emqConfig.tag = EMQTestConfig.EMQ_TAG;
        testReceiveMessage();
        QueueUtil.deleteTag(queueName);
    }

}
