package storm.emq;

import com.xiaomi.infra.galaxy.emq.thrift.ReceiveMessageRequest;

import java.io.Serializable;

/**
 * Created by jiasheng on 15-12-24.
 */
public interface EMQCoordinator extends Serializable {

    ReceiveMessageRequest newReceiveMessageRequest();

}
