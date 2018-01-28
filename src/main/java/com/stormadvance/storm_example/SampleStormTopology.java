package com.stormadvance.storm_example;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.bolt.JoinBolt;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;

import java.util.concurrent.TimeUnit;

public class SampleStormTopology {
	public static void main(String[] args) throws AlreadyAliveException,
			InvalidTopologyException {
		// create an instance of TopologyBuilder class
		TopologyBuilder builder = new TopologyBuilder();
		// set the spout class
		builder.setSpout("TwitterSpout", new TwitterSpout("/root/ptweets.txt"), 1);
		builder.setBolt("brandNERBolt",new BrandNERBolt("/root/brand_crf.model"),4).shuffleGrouping("TwitterSpout");
		builder.setBolt("productNERBolt",new ProductNERBolt("/root/product_crf.model"),4).shuffleGrouping("TwitterSpout");
		builder.setBolt("GroupClassificationBolt",new GroupClassificationBolt("/root/group.model.LogReg"),4).shuffleGrouping("TwitterSpout");
		builder.setBolt("StateClassificationBolt",new GroupClassificationBolt("/root/status.model.LogReg"),4).shuffleGrouping("TwitterSpout");



		JoinBolt nerJoiner = new JoinBolt("brandNERBolt", "id")
				.join("productNERBolt",    "id","brandNERBolt")
				.select ("id,brandset,productset")
				.withTumblingWindow( new BaseWindowedBolt.Duration(10, TimeUnit.SECONDS) );
		builder.setBolt("nerjoiner", nerJoiner)
				.fieldsGrouping("brandNERBolt", new Fields("id"))
				.fieldsGrouping("productNERBolt", new Fields("id"));

		/*JoinBolt classifierJoiner = new JoinBolt("GroupClassificationBolt", "id")
				.join("StateClassificationBolt",    "id","GroupClassificationBolt")
				.select ("GroupClassificationBolt:id,group,status")
				.withTumblingWindow( new BaseWindowedBolt.Duration(10, TimeUnit.SECONDS) );
		builder.setBolt("classifierJoiner", classifierJoiner)
				.fieldsGrouping("GroupClassificationBolt", new Fields("id"))
				.fieldsGrouping("StateClassificationBolt", new Fields("id"));

		JoinBolt IEJoiner = new JoinBolt("nerjoiner", "id")
				.join("StateClassificationBolt",    "id","GroupClassificationBolt")
				.select ("GroupClassificationBolt:id,group,status")
				.withTumblingWindow( new BaseWindowedBolt.Duration(10, TimeUnit.SECONDS) );
		builder.setBolt("classifierJoiner", classifierJoiner)
				.fieldsGrouping("GroupClassificationBolt", new Fields("id"))
				.fieldsGrouping("StateClassificationBolt", new Fields("id"));*/

		builder.setBolt("printer", new PrinterBolt() ).shuffleGrouping("nerjoiner");

		// set the bolt class
		/*builder.setBolt("SampleBolt", new SampleBolt(), 4).shuffleGrouping(
				"TwitterSpout");*/
		Config conf = new Config();
		conf.setDebug(true);
		// create an instance of LocalCluster class for
		// executing topology in local mode.
		LocalCluster cluster = new LocalCluster();
		// SampleStormTopology is the name of submitted topology
		cluster.submitTopology("SampleStormTopology", conf,
				builder.createTopology());
		try {
			Thread.sleep(1000000);
		} catch (Exception exception) {
			System.out.println("Thread interrupted exception : " + exception);
		}
		// kill the SampleStormTopology
		cluster.killTopology("SampleStormTopology");
		// shutdown the storm test cluster
		cluster.shutdown();
	}
}
