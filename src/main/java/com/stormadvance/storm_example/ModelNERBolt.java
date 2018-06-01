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
	private long initiatatedTime;
	private long threadid;
	private long count;
	//BufferedReader br;

	public final void prepare(final Map map,
			final TopologyContext topologyContext,
			final OutputCollector collector) {
		initiatatedTime = System.nanoTime() - (24 * 60 * 60 * 1000 * 1000 * 1000);
		threadid=Thread.currentThread().getId();
		count = 1;
		this.collector = collector;
		// Bolt will read the AFINN Sentiment file [which is in the classpath]
		// and stores the key, value pairs to a Map.


		//br = new BufferedReader(new FileReader(path));



	}

	public final void declareOutputFields(
			final OutputFieldsDeclarer outputFieldsDeclarer) {
		outputFieldsDeclarer.declare(new Fields("id","modelset","TID_MOD","TT_MOD","AV_MOD","CNT_MOD"));
	}

	public final void execute(final Tuple input) {
		long beforeProcessTS = System.nanoTime() - (24 * 60 * 60 * 1000 * 1000 * 1000);

		String row=input.getStringByField("tweet");
		int id=input.getIntegerByField("id");




		Set<String> modelSet = new HashSet<String>();
		for(String token:row.split(" ")){
			if(isAlphanumeric(token)){
				modelSet.add(token);
			}

		}
		Long afterProcessTS = System.nanoTime() - (24 * 60 * 60 * 1000 * 1000 * 1000);
		long averageTS = (afterProcessTS - initiatatedTime) / count;
		count++;
		long timeTaken = afterProcessTS - beforeProcessTS;
		if(modelSet.size()>0){
			collector.emit( new Values(id,modelSet,threadid,timeTaken,averageTS,count));
		}
		else {
			modelSet.add("nomodel");
			collector.emit( new Values(id,modelSet,threadid,timeTaken,averageTS,count));

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
