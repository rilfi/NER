package extra;

import com.aliasi.chunk.Chunk;
import com.aliasi.chunk.Chunking;
import com.aliasi.crf.ChainCrfChunker;
import com.aliasi.util.AbstractExternalizable;

import java.io.*;
import java.util.*;

/**
 * Created by s1 on 7/2/2017.
 */
public class ProductNERTest {
    public static void main(String[] args) throws IOException, ClassNotFoundException {
        int count=0;
        int found=0;
       File modelFile1 = new File("product_crf.model");
        File modelFile2 = new File("brand_crf.model");
        List<ChainCrfChunker>ch=new ArrayList<ChainCrfChunker>();
        ChainCrfChunker crfChunker1 = (ChainCrfChunker) AbstractExternalizable.readObject(modelFile1);
        ChainCrfChunker crfChunker2 = (ChainCrfChunker) AbstractExternalizable.readObject(modelFile2);
        ch.add(crfChunker1);
        ch.add(crfChunker2);
        BufferedReader br=new BufferedReader(new FileReader("tweetsall.txt"));
        BufferedWriter writer = new BufferedWriter(new FileWriter("ptweets.txt"));
        String line;
        while ((line=br.readLine())!=null){
            count++;
            Map<String,Set<String>> allmap=new HashMap<String, Set<String>>();
            for(ChainCrfChunker c:ch){
                Chunking chunking = c.chunk(line);
                Set<String> brandSet = new HashSet<String>();
                Set<String> productSet = new HashSet<String>();
                for (Chunk el : chunking.chunkSet()) {
                    int start = el.start();
                    int end = el.end();
                    String chuntText = (String) chunking.charSequence().subSequence(start, end);
                    String type = el.type();
                    if (type.equals("brand")) {
                        brandSet.add(chuntText.toLowerCase());
                    }
                    else if(type.equals("category")) {
                        productSet.add(chuntText.toLowerCase());
                    }
                }
                if(!brandSet.isEmpty()) {
                    allmap.put("brand", brandSet);
                }
                if(!productSet.isEmpty()){
                    allmap.put("product",productSet);
                }
            }
            if(allmap.size()==2){
                found++;

                System.out.println(count+"--"+found+"--"+line+"--  "+allmap+"--");
                writer.write(line);
                writer.newLine();
                writer.flush();

            }



        }
        br.close();
        writer.close();







    }

}
