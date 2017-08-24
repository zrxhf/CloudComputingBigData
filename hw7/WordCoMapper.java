package stubs;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCoMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    
	    String line = value.toString();
        String lineSplit[] = line.split("\\W+");
	    for (int i =0;i<lineSplit.length-1;i++) {
	        String word1 = lineSplit[i];
	        String word2 = lineSplit[i+1];
	      if (word1.length() > 0 && word2.length()>0) {	        
	        context.write(new Text(word1.toLowerCase()+","+word2.toLowerCase()), new IntWritable(1));
	      }
	    }
    
  }
}
