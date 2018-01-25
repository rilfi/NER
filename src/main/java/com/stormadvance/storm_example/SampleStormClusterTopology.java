package com.stormadvance.storm_example;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;

public class SampleStormClusterTopology {
	public static void main(String[] args) throws AlreadyAliveException,
			InvalidTopologyException {
		// create an instance of TopologyBuilder class
		TopologyBuilder builder = new TopologyBuilder();
		// set the spout class
		builder.setSpout("SampleSpout", new SampleSpout(), 2);
		//builder.setBolt("CRFBolt",new CRFBolt("/root/brand_crf.model"),2).shuffleGrouping("SampleSpout");
        builder.setBolt("CRFBolt",new SentimentBolt("/root/brand_crf.model"),2).shuffleGrouping("SampleSpout");
		// set the bolt class
		/*builder.setBolt("SampleBolt", new SampleBolt(), 4).shuffleGrouping(
				"SampleSpout");*/
		/*builder.setBolt("PersistenceBolt", new PersistenceBolt(args[0], args[1], args[2], args[3]), 1).shuffleGrouping(
				"SampleBolt");*/
		Config conf = new Config();
		conf.setNumWorkers(4);
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
