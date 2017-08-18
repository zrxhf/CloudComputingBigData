
package stubs;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Example input line:
 * 96.7.4.14 - - [24/Apr/2011:04:20:11 -0400] "GET /cat.jpg HTTP/1.1" 200 12433
 *
 */
public class LogFileMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {

	  String line = value.toString();
	  int charIndex = 0;
	  for(int i =0;i<line.length();i++){
		  if(line.charAt(i)==' '){
			  charIndex = i;
			  break;
		  }
	  }
	  
	  context.write(new Text(line.substring(0, charIndex)), new IntWritable(1));
	  

  }
}