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
    private long started;
    int count1;
    int count2;
	public TwitterSpout(String path){
        this.path = path;

    }

	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector spoutOutputCollector) {
		// Open the spout
		this.spoutOutputCollector = spoutOutputCollector;
        started = System.nanoTime() - (24 * 60 * 60 * 1000 * 1000 * 1000);
        count1=0;
        count2=0;
		id=1;
        try {
             br = new BufferedReader(new FileReader(path));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

	public void nextTuple() {
        count1++;
        String line;
        long beforeProcessTS = System.nanoTime() - (24 * 60 * 60 * 1000 * 1000 * 1000);
        try {
            if((line=br.readLine())!=null) {
                spoutOutputCollector.emit(new Values(id, line.toLowerCase(),started,beforeProcessTS));
                count2++;
                try{
                    Thread.sleep(1000);
                }catch(Exception e) {
                    System.out.println("Failed to sleep the thread");
                }
            }



        } catch (IOException e) {

            e.printStackTrace();
            //System.exit(1);
        }

		id++;

	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {

		// emit the tuple with field "site"
		declarer.declare(new Fields("id","tweet","Started","TPLSTART"));
	}
}
