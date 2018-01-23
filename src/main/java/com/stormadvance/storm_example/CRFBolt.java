package com.stormadvance.storm_example;

import com.aliasi.chunk.Chunk;
import com.aliasi.chunk.Chunking;
import com.aliasi.crf.ChainCrfChunker;
import com.aliasi.util.AbstractExternalizable;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;


/*import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;
import java.util.logging.Logger;*/

public class CRFBolt extends BaseBasicBolt {
	private static final long serialVersionUID = 1L;
	private OutputCollector collector;
	private String path;
	File modelFile ;
	ChainCrfChunker crfChunker;
	public CRFBolt(String path) {
		this.path = path;
	}
	private Map<String, Integer> NERMap = new HashMap<String, Integer>();

	public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
		this.collector = collector;
		modelFile = new File(path);
		try {
			crfChunker= (ChainCrfChunker) AbstractExternalizable.readObject(modelFile);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}


	}



		public void execute(Tuple input, BasicOutputCollector collector) {

			String row=input.getStringByField("row");
			Chunking chunking = crfChunker.chunk(row);
			Set<String> brandSet=new HashSet<String>();
			Set<String>catSet=new HashSet<String>();
			Map<String,Set<String>> returnMap=new HashMap<String, Set<String>>();
			for(Chunk el:chunking.chunkSet()){
				int start=el.start();
				int end=el.end();
				String chuntText= (String) chunking.charSequence().subSequence(start,end);
				String type=el.type();
				if(type.equals("brand")){
					brandSet.add(chuntText.toLowerCase());
				}
				else if(type.equals("CAT")){
					catSet.add(chuntText.toLowerCase());
				}
			}
			if(brandSet.size()>0){
				returnMap.put("BND",brandSet);

			}
			if (catSet.size()>0){
				returnMap.put("category",catSet);
			}
			if(returnMap.size()>0){
				collector.emit( new Values(row,returnMap));
			}


	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {

		declarer.declare(new Fields("row","ner"));
	}

}
