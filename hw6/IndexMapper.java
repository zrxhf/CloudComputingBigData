package stubs;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class IndexMapper extends Mapper<Text, Text, Text, Text> {

  @Override
  public void map(Text key, Text value, Context context) throws IOException,
      InterruptedException {
	  
String line = value.toString();

String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();

for (String word : line.split("\\W+")) {
    if (word.length() > 0) {

      context.write(new Text(word.toLowerCase()), new Text(fileName+"@"+key));
    }
  }




  }
}