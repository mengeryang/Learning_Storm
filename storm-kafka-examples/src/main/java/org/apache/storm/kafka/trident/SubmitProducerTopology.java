package org.apache.storm.kafka.trident;

import static org.apache.storm.kafka.spout.KafkaSpoutConfig.FirstPollOffsetStrategy.EARLIEST;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.kafka.trident.KafkaProducerTopology;
import org.apache.storm.kafka.spout.KafkaSpoutRetryExponentialBackoff.TimeInterval;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

public class SubmitProducerTopology {
    private static final String TOPIC = "storm-kafka-test";
    private static final String DEFAULT_BROKER_URL = "localhost:9092";

    public static void main(String[] args) throws Exception {
        new SubmitProducerTopology().runMain(args);
    }

    protected void runMain(String[] args) throws Exception {
        final String broker_url = args.length > 0 ? args[0] : DEFAULT_BROKER_URL;
        System.out.println("Running with broker url: " + broker_url);

        Config tpconf = getConfig();
        StormSubmitter.submitTopology(TOPIC + "-producer", tpconf, KafkaProducerTopology.newTopology(broker_url, TOPIC));
    }

    protected Config getConfig() {
        Config conf = new Config();
        conf.setDebug(true);
        return conf;
    }
}
