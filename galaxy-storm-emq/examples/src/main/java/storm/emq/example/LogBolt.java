package storm.emq.example;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;

/**
 * Created by jiasheng on 15-12-29.
 */
public class LogBolt extends BaseBasicBolt {
    private final Logger LOGGER = LoggerFactory.getLogger(LogBolt.class);
    private final Set<String> cache = new HashSet<String>();
    private final Random random = new Random();

    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        String body = tuple.getString(0);
        cache.add(body);
        if (random.nextInt(100) > 95) {
            LOGGER.info("Message body: " + body);
            LOGGER.info("Message cache size: " + cache.size());
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
