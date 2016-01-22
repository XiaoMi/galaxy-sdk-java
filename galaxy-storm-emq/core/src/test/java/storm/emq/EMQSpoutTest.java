package storm.emq;

import com.xiaomi.infra.galaxy.emq.client.EMQClientFactory;
import com.xiaomi.infra.galaxy.emq.thrift.MessageService;
import com.xiaomi.infra.galaxy.emq.thrift.SendMessageRequest;
import libthrift091.TException;
import org.junit.*;

/**
 * Created by jiasheng on 15-12-28.
 */
public class EMQSpoutTest {
    private static String queueName;
    private MockSpoutExecutor executor;

    @BeforeClass
    public static void beforeClass() throws TException {
        queueName = QueueUtil.createQueue();
        QueueUtil.createTag(queueName);
    }

    @AfterClass
    public static void afterClass() throws TException {
        QueueUtil.deleteTag(queueName);
        QueueUtil.deleteQueue(queueName);
    }

    @Before
    public void setup() {
        executor = new MockSpoutExecutor(new EMQSpout(new EMQConfig(
                EMQTestConfig.EMQ_ENDPOINT,
                queueName,
                EMQTestConfig.EMQ_TAG,
                QueueUtil.getCredential())));
    }

    @After
    public void tearDown() {
        executor.stop();
    }


    @Test
    public void testEMQSpout() throws InterruptedException {
        int sent = 0;
        MessageService.Iface messageClient = new EMQClientFactory(QueueUtil.getCredential()).newMessageClient(EMQTestConfig.EMQ_ENDPOINT);
        for (int i = 0; i < 100; i++) {
            SendMessageRequest request = new SendMessageRequest(queueName, i + "");
            try {
                messageClient.sendMessage(request);
                sent++;
            } catch (TException e) {
            }
        }
        executor.exec();
        System.out.println("Emited: " + executor.getEmitCount() + "\nAcked: " + executor.getAckCount() + "\nFailed: " + executor.getFailCount());
        Assert.assertTrue("Emited messages number of EMQSpout should be more than messages sent", executor.getEmitCount() >= sent);
    }
}
