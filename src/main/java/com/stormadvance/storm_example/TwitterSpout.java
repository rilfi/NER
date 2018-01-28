package com.stormadvance.storm_example;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

public class TwitterSpout extends BaseRichSpout {
	private static final long serialVersionUID = 1L;


	private SpoutOutputCollector spoutOutputCollector;
	int id;
    BufferedReader br;
	private String path;
	public TwitterSpout(String path){
        this.path = path;

    }

	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector spoutOutputCollector) {
		// Open the spout
		this.spoutOutputCollector = spoutOutputCollector;
		id=1;
        try {
             br = new BufferedReader(new FileReader(path));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

	public void nextTuple() {
        String line;
        try {
            if((line=br.readLine())!=null) {
                spoutOutputCollector.emit(new Values(id, line.toLowerCase()));
                try{
                    Thread.sleep(5000);
                }catch(Exception e) {
                    System.out.println("Failed to sleep the thread");
                }
            }



        } catch (IOException e) {

            e.printStackTrace();
            System.exit(1);
        }

		id++;

	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {

		// emit the tuple with field "site"
		declarer.declare(new Fields("id","tweet"));
	}
}
