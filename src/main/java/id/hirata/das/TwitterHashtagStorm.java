package id.hirata.das;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

import static java.util.Arrays.copyOfRange;

public class TwitterHashtagStorm {
    public static void main(String[] args) throws Exception{
        //String consumerKey = args[0];
        String consumerKey = "8daRE4pe8hnSBNStJN2qX6uDJ";
        //String consumerSecret = args[1];
        String consumerSecret = "9rYrehTV5dJjH0fSoAKfA5paGHdxtE5azihDRsY9iyX44IREXL";

        //String accessToken = args[2];
        //String accessTokenSecret = args[3];
        String accessToken = "100682295-q4pNafmxXUdPkuL4A86VVMqjqQmYeYSEfgp5AImx";
        String accessTokenSecret = "1ZXZT5RhiHHLm5QjNgkSH3yp12W7Sb9EqiCKWYZrh6EET";

        //String[] arguments = args.clone();
        String[] keyWords = {"music","movie"};

        Config config = new Config();
        config.setDebug(false);

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("twitter-spout", new TwitterSpout(consumerKey,
                consumerSecret, accessToken, accessTokenSecret, keyWords));

        builder.setBolt("twitter-hashtag-reader-bolt", new HashtagReaderBolt())
                .shuffleGrouping("twitter-spout");

        builder.setBolt("twitter-hashtag-counter-bolt", new HashtagCounterBolt())
                .fieldsGrouping("twitter-hashtag-reader-bolt", new Fields("hashtag"));

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("TwitterHashtagStorm", config,
                builder.createTopology());
        Thread.sleep(20000);
        cluster.shutdown();
    }
}