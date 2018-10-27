package br.ufpr.inf.lbd.examples;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.Iterator;

/**
 * GroupBy
 * select count(*) from lineitem group by l_shipmode;
 */
public class GroupBy {

  public static class GroupByMapper implements Mapper<LongWritable, Text, Text, IntWritable> {
    private Text _key = new Text();
    private final static IntWritable one = new IntWritable(1);

    @Override
    public void configure(JobConf jobConf) {
    }

    @Override
    public void close() throws IOException {
    }

    // this is one tpc-h lineitem entry:
    // 21318|10128|5131|1|33|34257.96|0.07|0.02|A|F|1993-10-03|1993-09-14|1993-10-26|DELIVER IN PERSON|TRUCK|its after the slyly ironic|
    public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
      String[] values = value.toString().split("\\|");
      _key.set(values[14].trim()); // get l_orderkey
      output.collect(_key, one);
    }
  }

  public static class GroupByReducer implements Reducer<Text, IntWritable, Text, IntWritable> {
    private IntWritable result = new IntWritable();
    private JobConf conf = new JobConf();


    @Override
    public void configure(JobConf jobConf) {
      this.conf = jobConf;
    }

    @Override
    public void close() throws IOException {
    }

    @Override
    public void reduce(Text key, Iterator<IntWritable> iterator, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
      int sum = 0;
      while(iterator.hasNext()) {
        IntWritable val = iterator.next();
        sum += val.get();
      }
      result.set(sum);
      output.collect(key, result);
    }
  }

  /**
   * Main
   */
  public static void main(String[] args) throws Exception {

    JobConf conf = new JobConf();

    conf.setJobName("GroupBy");

    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length != 2) {
      System.err.println("Usage: hadoop jar mrexamples.jar br.ufpr.inf.lbd.examples.GroupBy <in> <out>");
      System.exit(2);
    }

    conf.setJarByClass(Aggregation.class);
    conf.setInputFormat(TextInputFormat.class);
    conf.setOutputFormat(TextOutputFormat.class);

    conf.setMapperClass(GroupByMapper.class);
    conf.setReducerClass(GroupByReducer.class);

    conf.setMapOutputKeyClass(Text.class);
    conf.setMapOutputValueClass(IntWritable.class);

    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(IntWritable.class);

    FileInputFormat.addInputPath(conf, new Path(otherArgs[0]));
    FileOutputFormat.setOutputPath(conf, new Path(otherArgs[1]));

    JobClient jc = new JobClient(conf);
    RunningJob job = jc.submitJob(conf);
    jc.monitorAndPrintJob(conf, job);
  }

}
