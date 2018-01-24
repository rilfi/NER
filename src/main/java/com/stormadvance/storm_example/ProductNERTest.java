package com.stormadvance.storm_example;

import com.aliasi.chunk.Chunk;
import com.aliasi.chunk.Chunking;
import com.aliasi.crf.ChainCrfChunker;
import com.aliasi.util.AbstractExternalizable;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by s1 on 7/2/2017.
 */
public class ProductNERTest {
    public static void main(String[] args) throws IOException, ClassNotFoundException {
       File modelFile = new File("product_crf.model");
        ChainCrfChunker crfChunker = (ChainCrfChunker) AbstractExternalizable.readObject(modelFile);
        /*Chunking chunking = crfChunker.chunk("hhgfd NUMARK 200FX Vocal Effects Mixer");
        Set<String> catSet=new HashSet<String>();
        for(Chunk el:chunking.chunkSet()){
            int start=el.start();
            int end=el.end();
            String chuntText= (String) chunking.charSequence().subSequence(start,end);
            String type=el.type();
            *//*if(type.equals("category")){
                System.out.println(chuntText.toLowerCase());
            }*//*
           *//* else if(type.equals("CAT")){
                catSet.add(chuntText.toLowerCase());
            }*//*

        }*/
        for (int i = 1; i < args.length; ++i) {
            Chunking chunking = crfChunker.chunk("hhgfd NUMARK 200FX Vocal Effects Mixer");
            System.out.println("Chunking=" + chunking);
        }




    }

}
