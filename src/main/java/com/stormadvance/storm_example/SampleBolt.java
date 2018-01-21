package com.stormadvance.storm_example;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;


/*import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;
import java.util.logging.Logger;*/

public class SampleBolt extends BaseBasicBolt {
	private static final long serialVersionUID = 1L;

	//BufferedWriter writer ;
	/*public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
		try {
			writer = new BufferedWriter(new FileWriter("f1.txt"));
		} catch (IOException e) {
			e.printStackTrace();
		}

	}*/



		public void execute(Tuple input, BasicOutputCollector collector) {
		// fetched the field "site" from input tuple.
		String test = input.getStringByField("site");
			/*File file = new File("/root/file.txt");
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
				bufferWritter.write(test);
				bufferWritter.newLine();
			} catch (IOException e) {
				e.printStackTrace();
			}
			try {
				bufferWritter.close();
			} catch (IOException e) {
				e.printStackTrace();
			}*/

			// print the value of field "site" on console.
			collector.emit(new Values(test));
		//LOGGER.debug("######### Name of input site is : ",test);
		//System.out.println("######### Name of input site is : " + test);
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("site"));
	}

}
