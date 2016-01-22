package storm.emq;

import com.xiaomi.infra.galaxy.emq.client.EMQClientFactory;
import com.xiaomi.infra.galaxy.emq.thrift.*;
import com.xiaomi.infra.galaxy.rpc.thrift.Credential;
import com.xiaomi.infra.galaxy.rpc.thrift.UserType;
import libthrift091.TException;

/**
 * Created by jiasheng on 15-12-30.
 */
public class QueueUtil {
    public static Credential getCredential() {
        return new Credential().setSecretKeyId(EMQTestConfig.EMQ_SECRET_KEY_ID)
                .setSecretKey(EMQTestConfig.EMQ_SECRET_KEY)
                .setType(UserType.APP_SECRET);
    }

    public static String createQueue() throws TException {
        String queueName = null;
        QueueService.Iface queueClient = new EMQClientFactory(getCredential()).newQueueClient(EMQTestConfig.EMQ_ENDPOINT);
        CreateQueueRequest createQueueRequest = new CreateQueueRequest(EMQTestConfig.EMQ_QUEUE_NAME);
        CreateQueueResponse response = null;
        try {
            response = queueClient.createQueue(createQueueRequest);
            queueName = response.getQueueName();
        } catch (TException e) {
            if (((GalaxyEmqServiceException) e).getErrMsg().equals("QueueExistException")) {
                String details = ((GalaxyEmqServiceException) e).getDetails();
                queueName = details.substring(details.lastIndexOf(":") + 1).trim();
                System.out.println("Queue is already exist: " + e);
            } else
                throw e;
        }
        return queueName;
    }

    public static void deleteQueue(String queueName) throws TException {
        QueueService.Iface queueClient = new EMQClientFactory(getCredential()).newQueueClient(EMQTestConfig.EMQ_ENDPOINT);
        DeleteQueueRequest deleteQueueRequest = new DeleteQueueRequest(queueName);
        queueClient.deleteQueue(deleteQueueRequest);
        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public static void createTag(String queueName) throws TException {
        QueueService.Iface queueClient = new EMQClientFactory(getCredential()).newQueueClient(EMQTestConfig.EMQ_ENDPOINT);
        CreateTagRequest request = new CreateTagRequest(queueName, EMQTestConfig.EMQ_TAG);
        queueClient.createTag(request);
    }

    public static void deleteTag(String queueName) throws TException {
        QueueService.Iface queueClient = new EMQClientFactory(getCredential()).newQueueClient(EMQTestConfig.EMQ_ENDPOINT);
        DeleteTagRequest request = new DeleteTagRequest(queueName, EMQTestConfig.EMQ_TAG);
        queueClient.deleteTag(request);
    }
}
