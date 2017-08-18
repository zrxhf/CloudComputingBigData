package stubs;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class LetterMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		// read next line
		String line = value.toString();
		// read each word
		for (String word : line.split("\\W+")) {
			if (word.length() > 0) {
				// output the first letter as key, and the word length as value
				context.write(new Text(word.substring(0, 1)), new IntWritable(
						word.length()));
			}
		}

	}
}
