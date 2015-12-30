package storm.emq;

import backtype.storm.spout.Scheme;
import com.xiaomi.infra.galaxy.emq.thrift.ReceiveMessageResponse;

import java.util.List;

/**
 * Created by jiasheng on 15-12-24.
 */
public abstract class EMQScheme implements Scheme {
    @Override
    public List<Object> deserialize(byte[] bytes) {
        throw new RuntimeException("Invalid call of deserialize");
    }

    public abstract List<Object> deserialize(ReceiveMessageResponse response);
}
