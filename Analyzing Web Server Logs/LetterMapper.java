package stubs;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class LetterMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	boolean caseS = false;
	@Override
	public void setup(Context context){
		Configuration conf = context.getConfiguration();
		caseS = conf.getBoolean("caseSensitive", true);
	}
	
	
	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		// read next line
		String line = value.toString();
		// read each word
		for (String word : line.split("\\W+")) {
			if (word.length() > 0) {
				char firstChar = word.charAt(0);
				if (caseS == false && firstChar >= 'A' && firstChar <= 'Z') {
					context.write(
							new Text(Character
									.toString((char) (firstChar + 32))),
							new IntWritable(word.length()));
				} else {
					context.write(new Text(word.substring(0, 1)),
							new IntWritable(word.length()));
				}

			}
		}
		
	}
}
