package storm.emq;

import backtype.storm.tuple.Fields;
import com.xiaomi.infra.galaxy.emq.thrift.ReceiveMessageResponse;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by jiasheng on 15-12-24.
 */
public class DefaultEMQScheme extends EMQScheme {
    public static final String EMQ_BODY = "body";
    public static final String EMQ_ATTRIBUTE = "attr";

    @Override
    public List<Object> deserialize(final ReceiveMessageResponse response) {
        return new ArrayList<Object>() {{
            add(response.getMessageBody());
            add(response.getAttributes());
        }};
    }

    @Override
    public Fields getOutputFields() {
        return new Fields(EMQ_BODY, EMQ_ATTRIBUTE);
    }
}
