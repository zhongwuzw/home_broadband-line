import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MaxTemperature {

  public static void main(String[] args) throws Exception {
    if (args.length != 2) {
      System.err.println("Usage: MaxTemperature <input path> <output path>");
      System.exit(-1);
    }
    
    Configuration conf = new Configuration();
    conf.set("fs.defaultFS", "hdfs://127.0.0.1:9000");    
    conf.set("hadoop.job.user","zhongwu"); 
    conf.set("mapreduce.framework.name","yarn");
        
//    conf.set("mapred.job.tracker","localhost:9010"); 
    conf.set("mapreduce.jobtracker.address","127.0.0.1:9010"); 
    conf.set("yarn.resourcemanager.hostname", "127.0.0.1");
    conf.set("yarn.resourcemanager.admin.address", "127.0.0.1:8033");
    conf.set("yarn.resourcemanager.address", "127.0.0.1:8032");
    conf.set("yarn.resourcemanager.resource-tracker.address", "127.0.0.1:8036");
    conf.set("yarn.resourcemanager.scheduler.address", "127.0.0.1:8030");
    
    Job job = new Job(conf, "zhongwu");
//    Job job = new Job();
    job.setJarByClass(MaxTemperature.class);
    job.setJobName("Max temperature");

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    
    job.setMapperClass(MaxTemperature.MaxTemperatureMapper.class);
    job.setCombinerClass(MaxTemperatureReducer.class);
    job.setReducerClass(MaxTemperatureReducer.class);
    
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
  
  public static class MaxTemperatureMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
		private static final int MISSING = 9999;
		
		@Override
		public void map(LongWritable key,Text value,Context context) throws IOException,InterruptedException {
		    String line = value.toString();
		    String year = line.substring(15, 19);
		    int airTemperature;
		    if (line.charAt(87) == '+') { // parseInt doesn't like leading plus signs
		      airTemperature = Integer.parseInt(line.substring(88, 92));
		    } else {
		      airTemperature = Integer.parseInt(line.substring(87, 92));
		    }
		    String quality = line.substring(92, 93);
		    if (airTemperature != MISSING && quality.matches("[01459]")) {
		      context.write(new Text(year), new IntWritable(airTemperature));
		    }
		}
	}
  
  public static class MaxTemperatureReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
  
	  @Override
	  public void reduce(Text key, Iterable<IntWritable> values,
	      Context context)
	      throws IOException, InterruptedException {
	    
	    int maxValue = Integer.MIN_VALUE;
	    for (IntWritable value : values) {
	      maxValue = Math.max(maxValue, value.get());
	    }
	    context.write(key, new IntWritable(maxValue));
	  }
	}
}