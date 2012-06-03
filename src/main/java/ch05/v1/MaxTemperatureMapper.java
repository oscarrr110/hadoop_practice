package ch05.v1;
// cc MaxTemperatureMapperV1 First version of a Mapper that passes MaxTemperatureMapperTest
import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
//vv MaxTemperatureMapperV1
public class MaxTemperatureMapper extends MapReduceBase
  implements Mapper<LongWritable, Text, Text, IntWritable> {
  
  public void map(LongWritable key, Text value,
      OutputCollector<Text, IntWritable> output, Reporter reporter)
      throws IOException {
    
    String line = value.toString();
    String id = line.substring(0, 1);
    int v = Integer.parseInt(line.substring(2));
    output.collect(new Text(id), new IntWritable(v));
  }
}
//^^ MaxTemperatureMapperV1
