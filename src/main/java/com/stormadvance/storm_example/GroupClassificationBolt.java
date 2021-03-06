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
import com.aliasi.classify.Classification;
import com.aliasi.classify.LogisticRegressionClassifier;
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
public final class GroupClassificationBolt extends BaseRichBolt {

	private static final long serialVersionUID = -5094673458112825122L;
	private OutputCollector collector;
	private String path;
	//BufferedReader br;
	File modelFile ;
	LogisticRegressionClassifier<CharSequence> classifier;
	private long initiatatedTime;
	private long threadid;
	private long count;
	public GroupClassificationBolt(String path) {
		this.path = path;
	}

	public final void prepare(final Map map,
							  final TopologyContext topologyContext,
							  final OutputCollector collector) {
		initiatatedTime = System.nanoTime() - (24 * 60 * 60 * 1000 * 1000 * 1000);
		threadid=Thread.currentThread().getId();
		count = 1;
		this.collector = collector;
		// Bolt will read the AFINN Sentiment file [which is in the classpath]
		// and stores the key, value pairs to a Map.

		modelFile = new File(path);
		try {
			classifier= (LogisticRegressionClassifier<CharSequence>) AbstractExternalizable.readObject(modelFile);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		//br = new BufferedReader(new FileReader(path));



	}

	public final void declareOutputFields(
			final OutputFieldsDeclarer outputFieldsDeclarer) {
		outputFieldsDeclarer.declare(new Fields("id","group","TID_GRO","TT_GRO","AV_GRO","CNT_GRO"));
	}

	public final void execute(final Tuple input) {
		long beforeProcessTS = System.nanoTime() - (24 * 60 * 60 * 1000 * 1000 * 1000);

		String row=input.getStringByField("tweet");
		int id=input.getIntegerByField("id");



		Classification classification
				= classifier.classify(row);
		String group=classification.bestCategory();


		Long afterProcessTS = System.nanoTime() - (24 * 60 * 60 * 1000 * 1000 * 1000);
		long averageTS = (afterProcessTS - initiatatedTime) / count;
		count++;
		long timeTaken = afterProcessTS - beforeProcessTS;

		if (group!=null) {
			collector.emit( new Values(id,group,threadid,timeTaken,averageTS,count));

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




}