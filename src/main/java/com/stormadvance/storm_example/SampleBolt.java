package com.stormadvance.storm_example;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;

public class SampleBolt extends BaseBasicBolt {
	private static final long serialVersionUID = 1L;
	BufferedWriter writer ;
	public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
		try {
			writer = new BufferedWriter(new FileWriter("f1.txt"));
		} catch (IOException e) {
			e.printStackTrace();
		}

	}



		public void execute(Tuple input, BasicOutputCollector collector) {
		// fetched the field "site" from input tuple.
		String test = input.getStringByField("site");
			File file = new File("file.txt");
			if (!file.exists()) {
				try {
					file.createNewFile();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			FileWriter fileWritter = null;
			try {
				fileWritter = new FileWriter(file.getName(),true);
			} catch (IOException e) {
				e.printStackTrace();
			}
			BufferedWriter bufferWritter = new BufferedWriter(fileWritter);
			try {
				bufferWritter.write(test+"  --  hgfhtdfgdsews");
			} catch (IOException e) {
				e.printStackTrace();
			}
			try {
				bufferWritter.close();
			} catch (IOException e) {
				e.printStackTrace();
			}

			// print the value of field "site" on console.
		//System.out.println("######### Name of input site is : " + test);
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}

}
