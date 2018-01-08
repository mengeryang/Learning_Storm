package org.apache.storm.kafka.trident;

import org.apache.storm.Config;
import org.apache.storm.LocalDRPC;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.trident.TridentState;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.BaseFilter;
import org.apache.storm.trident.operation.builtin.Count;
import org.apache.storm.trident.operation.builtin.FilterNull;
import org.apache.storm.trident.operation.builtin.MapGet;
import org.apache.storm.trident.spout.ITridentDataSource;
import org.apache.storm.trident.Stream;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Fields;
import org.apache.storm.starter.trident.TridentWordCount.Split;
import org.apache.storm.starter.trident.DebugMemoryMapState;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

public class SubmitConsumerTopology {

    protected static final Logger LOG = LoggerFactory.getLogger(SubmitConsumerTopology.class);

    public static void main(String[] args) throws Exception {
        final String zkURL = args[0];
        final String brokerURL = args[1];
        final String topic = args[2];
        Config tpconf = LocalSubmitter.defaultConfig();

        StormSubmitter.submitTopology(topic + "-consumer", tpconf,
                newTopology(new TransactionalTridentKafkaSpout(newTridentKafkaConfig(zkURL, topic))));

        Thread.sleep(2000);
        DrpcResultsPrinter.remoteClient().printResults(60, 1, TimeUnit.SECONDS);
    }

    public static StormTopology newTopology(ITridentDataSource tridentSpout) {
        return newTopology(null, tridentSpout);
    }

    public static StormTopology newTopology(LocalDRPC drpc, ITridentDataSource tridentSpout) {
        final TridentTopology tridentTopology = new TridentTopology();
        addDRPCStream(tridentTopology, addTridentState(tridentTopology, tridentSpout), drpc);
        return tridentTopology.build();
    }

    private static TridentKafkaConfig newTridentKafkaConfig(String zkURL, String topic) {
        ZkHosts hosts = new ZkHosts(zkURL);
        TridentKafkaConfig config = new TridentKafkaConfig(hosts, topic);
        config.scheme = new SchemeAsMultiScheme(new StringScheme());
        config.startOffsetTime = kafka.api.OffsetRequest.EarliestTime();
        return config;
    }

    private static Stream addDRPCStream(TridentTopology tridentTopology, TridentState tridentState, LocalDRPC drpc) {
        return tridentTopology.newDRPCStream("words", drpc)
                .each(new Fields("args"), new Split(), new Fields("word"))
                .groupBy(new Fields("word"))
                .stateQuery(tridentState, new Fields("word"), new MapGet(), new Fields("count"))
                .each(new Fields("count"), new FilterNull())
                .project(new Fields("word", "count"))
                .filter(new BaseFilter() {
                    @Override
                    public boolean isKeep(TridentTuple tuple) {
                        LOG.debug("DRPC RESULT: " + tuple);
                        return true;
                    }
                });
    }
    private static TridentState addTridentState(TridentTopology tridentTopology, ITridentDataSource tridentSpout) {
        final Stream spoutStream = tridentTopology.newStream("spout1", tridentSpout).parallelismHint(6);

        return    spoutStream.each(new Fields("sentence"), new Split(), new Fields("word"))
                        .groupBy(new Fields("word"))
                        .persistentAggregate(new DebugMemoryMapState.Factory(), new Count(), new Fields("count"));
    }


}
