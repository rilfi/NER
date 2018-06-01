package com.stormadvance.storm_example;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.bolt.JoinBolt;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;

import java.util.concurrent.TimeUnit;

public class SampleStormClusterTopology {
	public static void main(String[] args) throws AlreadyAliveException,
			InvalidTopologyException {
		// create an instance of TopologyBuilder class
		TopologyBuilder builder = new TopologyBuilder();
		// set the spout class
		//builder.setSpout("TwitterSpout", new TwitterSpout("/root/tweets100.txt"), 2);
		//builder.setBolt("CRFBolt",new CRFBolt("/root/brand_crf.model"),2).shuffleGrouping("TwitterSpout");
       // builder.setBolt("CRFBolt",new SentimentBolt("/root/brand_crf.model"),2).shuffleGrouping("TwitterSpout");
		// set the bolt class
		/*builder.setBolt("SampleBolt", new SampleBolt(), 4).shuffleGrouping(
				"TwitterSpout");*/
		/*builder.setBolt("PersistenceBolt", new PersistenceBolt(args[0], args[1], args[2], args[3]), 1).shuffleGrouping(
				"SampleBolt");*/

		builder.setSpout("TwitterSpout", new TwitterSpout("/root/ptweets.txt"), 1);
		builder.setBolt("brandNERBolt",new BrandNERBolt("/root/brand_crf.model"),6).setNumTasks(2).shuffleGrouping("TwitterSpout");
		//builder.setBolt("productNERBolt",new ProductNERBolt("/root/product_crf.model"),5).setNumTasks(2).shuffleGrouping("TwitterSpout");
		//builder.setBolt("GroupClassificationBolt",new GroupClassificationBolt("/root/group.model.LogReg"),5).setNumTasks(2).shuffleGrouping("TwitterSpout");
		//builder.setBolt("StateClassificationBolt",new StateClassificationBolt("/root/status.model.LogReg"),5).setNumTasks(2).shuffleGrouping("TwitterSpout");
		//builder.setBolt("ModelRecognizerBolt",new ModelNERBolt(),5).setNumTasks(2).shuffleGrouping("TwitterSpout");
		builder.setBolt("das",new RT_Das_rich_Bolt("/root/models/security/client-truststore.jks"),1).shuffleGrouping("brandNERBolt");




//		JoinBolt nerJoiner = new JoinBolt("brandNERBolt", "id")
//				.join("productNERBolt",    "id","brandNERBolt")
//				.select ("id,tweet,brandset,productset")
//				.withTumblingWindow( new BaseWindowedBolt.Duration(10, TimeUnit.SECONDS) );
//		builder.setBolt("nerjoiner", nerJoiner)
//				.fieldsGrouping("brandNERBolt", new Fields("id"))
//				.fieldsGrouping("productNERBolt", new Fields("id"));
//
//		JoinBolt classifierJoiner = new JoinBolt("GroupClassificationBolt", "id")
//				.join("StateClassificationBolt",    "id","GroupClassificationBolt")
//				.select ("id,group,status")
//				.withTumblingWindow( new BaseWindowedBolt.Duration(10, TimeUnit.SECONDS) );
//		builder.setBolt("classifierJoiner", classifierJoiner)
//				.fieldsGrouping("GroupClassificationBolt", new Fields("id"))
//				.fieldsGrouping("StateClassificationBolt", new Fields("id"));
//
//		JoinBolt IEJoiner = new JoinBolt("nerjoiner", "id")
//				.join("classifierJoiner",    "id","nerjoiner")
//				.select ("id,tweet,brandset,productset,group,status")
//				.withTumblingWindow( new BaseWindowedBolt.Duration(10, TimeUnit.SECONDS) );
//		builder.setBolt("IEJoiner", IEJoiner)
//				.fieldsGrouping("nerjoiner", new Fields("id"))
//				.fieldsGrouping("classifierJoiner", new Fields("id"));
//
//		JoinBolt finalJoiner = new JoinBolt("IEJoiner", "id")
//				.join("ModelRecognizerBolt",    "id","IEJoiner")
//				.select ("id,tweet,brandset,productset,group,status,modelset")
//				.withTumblingWindow( new BaseWindowedBolt.Duration(10, TimeUnit.SECONDS) );
//		builder.setBolt("finalJoiner", finalJoiner)
//				.fieldsGrouping("IEJoiner", new Fields("id"))
//				.fieldsGrouping("ModelRecognizerBolt", new Fields("id"));
		Config conf = new Config();
		conf.setNumWorkers(3);

		// This statement submit the topology on remote
		// args[0] = name of topology
		try {
			StormSubmitter.submitTopology(args[0], conf,
					builder.createTopology());
		} catch (AlreadyAliveException alreadyAliveException) {
			System.out.println(alreadyAliveException);
		} catch (InvalidTopologyException invalidTopologyException) {
			System.out.println(invalidTopologyException);
		} catch (AuthorizationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
