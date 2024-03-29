package ch05.v6;
//== MaxTemperatureDriverV6

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

import ch05.v1.MaxTemperatureReducer;
import ch05.v5.MaxTemperatureMapper;

public class MaxTemperatureDriver extends Configured implements Tool {

  public int run(String[] args) throws Exception {
    if (args.length != 2) {
      System.err.printf("Usage: %s [generic options] <input> <output>\n",
          getClass().getSimpleName());
      ToolRunner.printGenericCommandUsage(System.err);
      return -1;
    }
    
    JobConf conf = new JobConf(getConf(), getClass());
    conf.setJobName("Max temperature");
    
    FileInputFormat.addInputPath(conf, new Path(args[0]));
    FileOutputFormat.setOutputPath(conf, new Path(args[1]));
    
    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(IntWritable.class);

    conf.setMapperClass(MaxTemperatureMapper.class);
    conf.setCombinerClass(MaxTemperatureReducer.class);
    conf.setReducerClass(MaxTemperatureReducer.class);
    
    //vv MaxTemperatureDriverV6
    conf.setProfileEnabled(true);
    conf.setProfileParams("-agentlib:hprof=cpu=samples,heap=sites,depth=6," +
    		"force=n,thread=y,verbose=n,file=%s");
    conf.setProfileTaskRange(true, "0-2");
    //^^ MaxTemperatureDriverV6

    JobClient.runJob(conf);
    return 0;
  }
  
  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(new MaxTemperatureDriver(), args);
    System.exit(exitCode);
  }
}

