package stubs;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

public class TestProcessLogs {
	MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;
	ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver;
	MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable> mapReduceDriver;
	@Before
	public void setUp() throws Exception {
		LogFileMapper mapper = new LogFileMapper();
		mapDriver = new MapDriver<LongWritable, Text, Text, IntWritable>();
		mapDriver.setMapper(mapper);

		SumReducer reducer = new SumReducer();
		reduceDriver = new ReduceDriver<Text, IntWritable, Text, IntWritable>();
		reduceDriver.setReducer(reducer);

		mapReduceDriver = new MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable>();
		mapReduceDriver.setMapper(mapper);
		mapReduceDriver.setReducer(reducer);
	}

	@Test
	public void testMapper() {
		mapDriver.withInput(new LongWritable(1), new Text(
				"96.7.4.14 - - [24/Apr/2011:04:20:11 -0400] \"GET /cat.jpg HTTP/1.1\" 200 12433"));
		mapDriver.withOutput(new Text("96.7.4.14"), new IntWritable(1));
		System.out.println("mapDriver Output is: "+mapDriver.getExpectedOutputs());
		mapDriver.runTest();
	}
	@Test
	public void testReducer() {
		List<IntWritable> valuest = new ArrayList<IntWritable>();
		valuest.add(new IntWritable(1));
		valuest.add(new IntWritable(1));
		valuest.add(new IntWritable(1));

		reduceDriver.withInput(new Text("10.216.113.172"), valuest);
		reduceDriver.withOutput(new Text("10.216.113.172"), new IntWritable(3));
		System.out.println("reduceDriver Output is: "+reduceDriver.getExpectedOutputs());
		reduceDriver.runTest();
	}
	@Test
	public void testMapReduce() {
		mapReduceDriver.withInput(new LongWritable(1), new Text(
				"10.223.157.186 - - [15/Jul/2009:21:24:17 -0700] \"GET /assets/img/media.jpg HTTP/1.1\" 200 110997"));
		mapReduceDriver.withOutput(new Text("10.223.157.186"), new IntWritable(1));

		System.out.println("mapReduceDriver Output is: "+mapReduceDriver.getExpectedOutputs());
		mapReduceDriver.runTest();

	}


}
