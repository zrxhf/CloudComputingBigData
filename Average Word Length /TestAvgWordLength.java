package stubs;

import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

public class TestAvgWordLength {
	MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;
	ReduceDriver<Text, IntWritable, Text, DoubleWritable> reduceDriver;
	MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, DoubleWritable> mapReduceDriver;

	@Before
	public void setUp() throws Exception {

		LetterMapper mapper = new LetterMapper();
		mapDriver = new MapDriver<LongWritable, Text, Text, IntWritable>();
		mapDriver.setMapper(mapper);

		AverageReducer reducer = new AverageReducer();
		reduceDriver = new ReduceDriver<Text, IntWritable, Text, DoubleWritable>();
		reduceDriver.setReducer(reducer);

		mapReduceDriver = new MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, DoubleWritable>();
		mapReduceDriver.setMapper(mapper);
		mapReduceDriver.setReducer(reducer);

	}

	@Test
	public void testMapper() {
		mapDriver.withInput(new LongWritable(1), new Text(
				"No now is definitely not the best time"));
		mapDriver.withOutput(new Text("N"), new IntWritable(2));
		mapDriver.withOutput(new Text("n"), new IntWritable(3));
		mapDriver.withOutput(new Text("i"), new IntWritable(2));
		mapDriver.withOutput(new Text("d"), new IntWritable(10));
		mapDriver.withOutput(new Text("n"), new IntWritable(3));
		mapDriver.withOutput(new Text("t"), new IntWritable(3));
		mapDriver.withOutput(new Text("b"), new IntWritable(4));
		mapDriver.withOutput(new Text("t"), new IntWritable(4));
		System.out.println("mapDriver Output is: "+mapDriver.getExpectedOutputs());
		mapDriver.runTest();
	}
	@Test
	public void testReducer() {
		List<IntWritable> valuest = new ArrayList<IntWritable>();
		valuest.add(new IntWritable(2));
		valuest.add(new IntWritable(3));
		valuest.add(new IntWritable(4));
		valuest.add(new IntWritable(5));
		
		reduceDriver.withInput(new Text("a"), valuest);
		reduceDriver.withOutput(new Text("a"), new DoubleWritable(3.5));
		System.out.println("reduceDriver Output is: "+reduceDriver.getExpectedOutputs());
		reduceDriver.runTest();
	}
	@Test
	public void testMapReduce() {
		mapReduceDriver.withInput(new LongWritable(1), new Text(
				"No now is definitely not the best time"));
		mapReduceDriver.withOutput(new Text("N"), new DoubleWritable(2.0));
		mapReduceDriver.withOutput(new Text("b"), new DoubleWritable(4.0));
		mapReduceDriver.withOutput(new Text("d"), new DoubleWritable(10.0));
		mapReduceDriver.withOutput(new Text("i"), new DoubleWritable(2.0));
		mapReduceDriver.withOutput(new Text("n"), new DoubleWritable(3.0));
		mapReduceDriver.withOutput(new Text("t"), new DoubleWritable(7.0/2.0));
		System.out.println("mapReduceDriver Output is: "+mapReduceDriver.getExpectedOutputs());
		mapReduceDriver.runTest();

	}

}
