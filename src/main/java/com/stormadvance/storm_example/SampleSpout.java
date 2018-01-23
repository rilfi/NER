package com.stormadvance.storm_example;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

public class SampleSpout extends BaseRichSpout {
	private static final long serialVersionUID = 1L;

	private static final Map<Integer, String> map = new HashMap<Integer, String>();
	static {
		map.put(0, "google");
		map.put(1, "facebook");
		map.put(2, "twitter");
		map.put(3, "youtube");
		map.put(4, "linkedin");
	}
	private SpoutOutputCollector spoutOutputCollector;

	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector spoutOutputCollector) {
		// Open the spout
		this.spoutOutputCollector = spoutOutputCollector;
	}

	public void nextTuple() {

		spoutOutputCollector.emit(new Values("Bling Shiny 3D Red Black Pink BOW Leopard Key Case Cover For Apple iPhone iPod Touch Samsung Galaxy Smart Mobile Phones (Black Bow, iPod Touch 4 4G 4th Gen)".toLowerCase()));
		try{
		Thread.sleep(5000);
		}catch(Exception e) {
			System.out.println("Failed to sleep the thread");
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {

		// emit the tuple with field "site"
		declarer.declare(new Fields("row"));
	}
}
