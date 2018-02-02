package org.apache.spark.streaming.talos;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import scala.Tuple2;

/**
 * Created by jiasheng on 16-3-25.
 */
public class JavaDirectTalosStreamSuite extends TalosClusterSuite implements Serializable {
    private transient JavaStreamingContext ssc = null;
    private transient TalosTestUtils talosTestUtils = null;
    private String topic = "spark-talos-java-test";

    @Before
    public void setUp() {
        SparkConf sparkConf = new SparkConf()
                .setMaster("local[4]").setAppName(this.getClass().getSimpleName());
        ssc = new JavaStreamingContext(sparkConf, Durations.milliseconds(200));
        talosTestUtils = new TalosTestUtils(uri(), new HashMap<String, String>() {{
            put("auto.offset.reset", "smallest");
        }});
        talosTestUtils.deleteTopic(topic);
        talosTestUtils.createTopic(topic, 8);
    }

    @After
    public void tearDown() {

    }

    @Test
    public void testDirectStream() throws InterruptedException {
        HashSet<String> sentMessages = new HashSet<String>();
        for (int i = 1; i <= 100; i++)
            sentMessages.add(i + "");
        talosTestUtils.sendMessagesAndWaitForReceive(topic, sentMessages);
        JavaPairDStream<String, String> stream = TalosUtils.createDirectStream(
                ssc, talosTestUtils.javaTalosParams(), talosTestUtils.credential(), new HashSet<String>() {{
                    add(topic);
                }}
        );
        final Set<String> result = Collections.synchronizedSet(new HashSet<String>());
        stream.foreachRDD(
                new Function<JavaPairRDD<String, String>, Void>() {
                    public Void call(JavaPairRDD<String, String> rdd) throws Exception {
                        Iterator<Tuple2<String, String>> iterator = rdd.collect().iterator();
                        while (iterator.hasNext()) {
                            result.add(iterator.next()._2);
                        }
                        return null;
                    }
                }
        );
        ssc.start();
        long startTime = System.currentTimeMillis();
        boolean matches = false;
        while (!matches && System.currentTimeMillis() - startTime < 20000) {
            matches = sentMessages.size() == result.size();
            Thread.sleep(50);
        }
        Assert.assertEquals(sentMessages, result);
        ssc.stop();
    }
}
