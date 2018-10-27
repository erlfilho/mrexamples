package br.ufpr.inf.lbd.examples;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

/**
 * Aggregation
 * select count(*) from lineitem;
 */
public class Aggregation {

  public static class AggregationMapper extends Mapper<Object, Text, IntWritable, IntWritable>{
    private final static IntWritable one = new IntWritable(1);

    // this is one tpc-h lineitem entry:
    // 21318|10128|5131|1|33|34257.96|0.07|0.02|A|F|1993-10-03|1993-09-14|1993-10-26|DELIVER IN PERSON|TRUCK|its after the slyly ironic|
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      context.write(one, one);
    }
  }

  public static class AggregationReducer extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable> {
    private IntWritable result = new IntWritable();
    public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(result, result);
    }
  }

  /**
   * Main
   */
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

    if (otherArgs.length != 2) {
      System.err.println("Usage: hadoop jar mrexamples.jar br.ufpr.inf.lbd.examples.Aggregation <in> <out>");
      System.exit(2);
    }

    Job job = new Job(conf, "Aggregation");
    job.setJarByClass(Aggregation.class);

    job.setMapperClass(AggregationMapper.class);
    // job.setCombinerClass(AggregationReducer.class);
    job.setReducerClass(AggregationReducer.class);

    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(IntWritable.class);

    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(IntWritable.class);

    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }

}
