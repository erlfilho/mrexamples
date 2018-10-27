package br.ufpr.inf.lbd.examples;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.Iterator;

public class Range {

  /**
   * Range
   * select * from lineitem where l_orderkey > 100000;
   */
  public static class RangeMapper implements Mapper<LongWritable, Text, LongWritable, Text> {
    private LongWritable _key = new LongWritable();

    @Override
    public void configure(JobConf jobConf) {
    }

    @Override
    public void close() throws IOException {
    }

    public void map(LongWritable key, Text value, OutputCollector<LongWritable, Text> output, Reporter reporter) throws IOException {
      System.out.println("LongWritable: "  + key);
      // get l_orderkey key and comparte to RAIL
      // max value of l_orderkey = 6000000
      String[] values = value.toString().split("\\|");

      int valueOfKey = Integer.valueOf(values[0].trim());
      _key.set(Long.valueOf(values[0].trim())); // get l_orderkey
      if (valueOfKey <= 100000) {
        output.collect(_key, value);
      }
    }
  }

  public static class RangeReducer implements Reducer<LongWritable, Text, LongWritable, Text> {
    private LongWritable keyLongWritable = new LongWritable();

    @Override
    public void configure(JobConf jobConf) {
    }

    @Override
    public void close() throws IOException {
    }

    public void reduce(LongWritable key, Iterator<Text> values, OutputCollector<LongWritable, Text> output, Reporter reporter) throws IOException {
      while(values.hasNext())
        output.collect(key, values.next());
    }
  }

  /**
   * Main
   */
  public static void main(String[] args) throws Exception {
    JobConf conf = new JobConf();
    conf.setJobName("Range");

    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length != 2) {
      System.err.println("Usage: hadoop jar mrexamples.jar br.ufpr.inf.lbd.examples.Range <in> <out>");
      System.exit(2);
    }

    conf.setJarByClass(Aggregation.class);
    conf.setInputFormat(TextInputFormat.class);
    conf.setOutputFormat(TextOutputFormat.class);

    conf.setMapperClass(RangeMapper.class);
    conf.setReducerClass(RangeReducer.class);

    conf.setMapOutputKeyClass(Text.class);
    conf.setMapOutputValueClass(Text.class);

    conf.setOutputKeyClass(LongWritable.class);
    conf.setOutputValueClass(Text.class);

    FileInputFormat.addInputPath(conf, new Path(otherArgs[0]));
    FileOutputFormat.setOutputPath(conf, new Path(otherArgs[1]));

    JobClient jc = new JobClient(conf);
    RunningJob job = jc.submitJob(conf);
    jc.monitorAndPrintJob(conf, job);
  }

}
