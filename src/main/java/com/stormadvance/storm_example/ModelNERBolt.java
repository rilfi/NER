package com.stormadvance.storm_example;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.aliasi.chunk.Chunk;
import com.aliasi.chunk.Chunking;
import com.aliasi.crf.ChainCrfChunker;
import com.aliasi.util.AbstractExternalizable;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;


/**
 * Breaks each tweet into words and calculates the sentiment of each tweet and
 * assocaites the sentiment value to the State and logs the same to the console
 * and also logs to the file.
 *
 * @author - centos
 */
public final class ModelNERBolt extends BaseRichBolt {

	private static final long serialVersionUID = -5094673458112825122L;
	private OutputCollector collector;
	private String path;
	//BufferedReader br;
	File modelFile ;
	ChainCrfChunker crfChunker;
	public ModelNERBolt(String path) {
		this.path = path;
	}
	private Map<String, Integer> afinnSentimentMap = new HashMap<String, Integer>();

	public final void prepare(final Map map,
			final TopologyContext topologyContext,
			final OutputCollector collector) {
		this.collector = collector;
		// Bolt will read the AFINN Sentiment file [which is in the classpath]
		// and stores the key, value pairs to a Map.

			modelFile = new File(path);
		try {
			crfChunker= (ChainCrfChunker) AbstractExternalizable.readObject(modelFile);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		//br = new BufferedReader(new FileReader(path));



	}

	public final void declareOutputFields(
			final OutputFieldsDeclarer outputFieldsDeclarer) {
		outputFieldsDeclarer.declare(new Fields("tweet","nermap"));
	}

	public final void execute(final Tuple input) {
		String row=input.getStringByField("tweet");




		Set<String> modelSet = new HashSet<String>();
		Map<String, Set<String>> returnMap = (Map<String, Set<String>>) input.getValueByField("nermap");
		for(String token:row.split(" ")){
			if(isAlphanumeric(token)){
				modelSet.add(token);
			}

		}
		if(modelSet.size()>0){
			returnMap.put("model",modelSet);
		}
		if (returnMap.size() > 0) {
			//System.out.println(returnMap.keySet());
			collector.emit( new Values(row,returnMap));
		}
			/*while ((line = br.readLine()) != null) {
				*//*String[] tabSplit = line.split(",");
				afinnSentimentMap.put(tabSplit[0],
						Integer.parseInt(tabSplit[1]));*//*
				System.out.println(line);
				break;
			}*/




		//final String tweet = (String) input.getValueByField("row");
		//final int sentimentCurrentTweet = getSentimentOfTweet(tweet);
		//collector.emit(new Values("---"+tweet,afinnSentimentMap.size()));

	}
	public boolean isAlphanumeric(String str) {
		boolean isAlpha=false;
		boolean isNumaric=false;
		for (int i = 0; i < str.length(); i++) {
			char c = str.charAt(i);
			if (Character.isDigit(c) )
				isNumaric=true;
			break;
		}
		for (int i = 0; i < str.length(); i++) {
			char c = str.charAt(i);
			if (Character.isLetter(c))
				isAlpha=true;
			break;
		}
		if(isAlpha&&isNumaric){
			return true;
		}

		return false;
	}




}
