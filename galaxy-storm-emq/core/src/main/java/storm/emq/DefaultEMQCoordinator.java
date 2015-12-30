package storm.emq;

import com.xiaomi.infra.galaxy.emq.thrift.ReceiveMessageRequest;

/**
 * Created by jiasheng on 15-12-28.
 */
public class DefaultEMQCoordinator implements EMQCoordinator {
    private final EMQConfig emqConfig;

    public DefaultEMQCoordinator(EMQConfig emqConfig) {
        this.emqConfig = emqConfig;
    }

    @Override
    public ReceiveMessageRequest newReceiveMessageRequest() {
        return new ReceiveMessageRequest(emqConfig.queueName) {
            {
                if (emqConfig.tag != null)
                    setTagName(emqConfig.tag);
            }
        };
    }
}
