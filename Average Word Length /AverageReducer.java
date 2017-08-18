package stubs;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AverageReducer extends
		Reducer<Text, IntWritable, Text, DoubleWritable> {

	@Override
	public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {

		int wordCount = 0;
		int lengthSum = 0;
		/*
		 * For each value in the set of values passed to us by the mapper:
		 */
		for (IntWritable value : values) {
			/*
			 * Add the value to the word length sum for this key. And add 1 to
			 * the word counter.
			 */
			lengthSum += value.get();
			wordCount++;
		}
		// Calculate the average word length and emit the key-value pair.
		context.write(key, new DoubleWritable(lengthSum * 1.0 / wordCount));

	}
}