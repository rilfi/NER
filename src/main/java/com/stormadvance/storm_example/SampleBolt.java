package com.stormadvance.storm_example;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;

public class SampleBolt extends BaseBasicBolt {
	private static final long serialVersionUID = 1L;
	File myfile = new File("/fl.txt");
	FileWriter writer ;
	public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
		try {
			writer = new FileWriter(myfile, true);
		} catch (IOException e) {
			e.printStackTrace();
		}

	}



		public void execute(Tuple input, BasicOutputCollector collector) {
		// fetched the field "site" from input tuple.
		String test = input.getStringByField("site");
			try {
				writer.write("Tomorrow will be cloudy.");
			} catch (IOException e) {
				e.printStackTrace();
			}

			// print the value of field "site" on console.
		System.out.println("######### Name of input site is : " + test);
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}
}
