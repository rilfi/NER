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
		builder.setBolt("StateClassificationBolt",new StateClassificationBolt("/root/status.model.LogReg"),4).shuffleGrouping("TwitterSpout");



		JoinBolt nerJoiner = new JoinBolt("brandNERBolt", "id")
				.join("productNERBolt",    "id","brandNERBolt")
				.select ("id,brandset,productset")
				.withTumblingWindow( new BaseWindowedBolt.Duration(10, TimeUnit.SECONDS) );
		builder.setBolt("nerjoiner", nerJoiner)
				.fieldsGrouping("brandNERBolt", new Fields("id"))
				.fieldsGrouping("productNERBolt", new Fields("id"));

		/*JoinBolt classifierJoiner = new JoinBolt("GroupClassificationBolt", "id")
				.join("StateClassificationBolt",    "id","GroupClassificationBolt")
				.select ("id,group,status")
				.withTumblingWindow( new BaseWindowedBolt.Duration(10, TimeUnit.SECONDS) );
		builder.setBolt("classifierJoiner", classifierJoiner)
				.fieldsGrouping("GroupClassificationBolt", new Fields("id"))
				.fieldsGrouping("StateClassificationBolt", new Fields("id"));*/

		/*JoinBolt IEJoiner = new JoinBolt("nerjoiner", "id")
				.join("classifierJoiner",    "id","nerjoiner")
				.select ("id,brandset,productset,group,status")
				.withTumblingWindow( new BaseWindowedBolt.Duration(10, TimeUnit.SECONDS) );
		builder.setBolt("IEJoiner", IEJoiner)
				.fieldsGrouping("nerjoiner", new Fields("id"))
				.fieldsGrouping("classifierJoiner", new Fields("id"));*/

		JoinBolt fainalJoiner = new JoinBolt("nerjoiner", "id")
				.join("TwitterSpout",    "id","nerjoiner")
				.select ("id,brandset,productset,tweet")
				.withTumblingWindow( new BaseWindowedBolt.Duration(10, TimeUnit.SECONDS) );
		builder.setBolt("fainalJoiner", fainalJoiner)
				.fieldsGrouping("nerjoiner", new Fields("id"))
				.fieldsGrouping("TwitterSpout", new Fields("id"));
		//builder.setBolt("twitterBolt",new TwitterBolt(),1).shuffleGrouping("fainalJoiner");


		builder.setBolt("printer", new PrinterBolt(),1 ).shuffleGrouping("fainalJoiner");

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
			Thread.sleep(100000);
		} catch (Exception exception) {
			System.out.println("Thread interrupted exception : " + exception);
		}
		// kill the SampleStormTopology
		cluster.killTopology("SampleStormTopology");
		// shutdown the storm test cluster
		cluster.shutdown();
	}
}
