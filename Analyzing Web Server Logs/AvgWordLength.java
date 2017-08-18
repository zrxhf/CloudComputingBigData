
package stubs;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

public class AvgWordLength extends Configured implements Tool {
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new Configuration(), new AvgWordLength(),
				args);
		System.exit(exitCode);
	}
  public int run(String[] args) throws Exception {

    if (args.length != 2) {
      System.out.printf("Usage: %s [generic options] <input dir> <output dir>\n",
				getClass().getSimpleName());
      return -1;
    }
    
    Configuration conf = this.getConf();
    //conf.setBoolean("caseSensitive", false);
    
    Job job = new Job(conf);

    job.setJarByClass(AvgWordLength.class);
    
    job.setJobName("Average Word Length");

    FileInputFormat.setInputPaths(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    
    job.setMapperClass(LetterMapper.class);
    job.setReducerClass(AverageReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(DoubleWritable.class);
    job.setMapOutputValueClass(IntWritable.class);

    boolean success = job.waitForCompletion(true);
    return success ? 0 : 1;
  }
}

