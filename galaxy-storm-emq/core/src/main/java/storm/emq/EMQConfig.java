package storm.emq;

import com.xiaomi.infra.galaxy.rpc.thrift.Credential;

import java.io.Serializable;

/**
 * Created by jiasheng on 15-12-23.
 */
public class EMQConfig implements Serializable {

    public final String queueName;
    public final Credential credential;
    public String endpoint = null;
    public String tag = null;
    public EMQCoordinator emqCoordinator = new DefaultEMQCoordinator(this);
    public EMQScheme emqScheme = new DefaultEMQScheme();


    /**
     * max message number in a <i>DeleteMessageBatchRequest</i>
     */
    public int deleteMessageMaxNumPerBatch = 100;
    /**
     * max delay in ms before  send a new <i>DeleteMessageBatchRequest</i> to EMQ
     */
    public long deleteMessageMaxDelayMs = 100;
    /**
     * timeout in ms for nextTuple() in <i>EMQSpout</i> when no new message to emit
     */
    public long generateTupleTimeoutMs = 50;


    public EMQConfig(String queueName, Credential credential) {
        this.queueName = queueName;
        this.credential = credential;
    }

    public EMQConfig(String endpoint, String queueName, String tag, Credential credential) {
        this.endpoint = endpoint;
        this.queueName = queueName;
        this.tag = tag;
        this.credential = credential;
    }
}
