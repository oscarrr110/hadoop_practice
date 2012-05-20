package ch02;

// cc MaxTemperatureMapper Mapper for maximum temperature example
// vv MaxTemperatureMapper
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class MaxOrderMoneyMapper extends MapReduceBase implements
		Mapper<LongWritable, Text, Text, IntWritable> {

	private static final int MISSING = 9999;

	public void map(LongWritable key, Text value,
			OutputCollector<Text, IntWritable> output, Reporter reporter)
			throws IOException {

		String line = value.toString();
		
		String[] dataArray = line.split(",");
		String money = dataArray[8];
//		String llOrderId = dataArray[0];
		String issueId = dataArray[4];
		
//		String id = "llOrderId " + llOrderId + " tbOrderId " + tbOrderId;
		output.collect(new Text(issueId), new IntWritable(Integer.parseInt(money)));
	}

}
