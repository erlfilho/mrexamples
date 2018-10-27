package br.ufpr.inf.lbd.examples;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.Iterator;

/**
 * Filter
 * select * from lineitem where l_shipmode = 'RAIL';
 */
public class Filter {

  public static class FilterMapper implements Mapper<LongWritable, Text, LongWritable, Text> {
    LongWritable valueOfKey = new LongWritable();

    @Override
    public void configure(JobConf jobConf) {
    }

    @Override
    public void close() throws IOException {
    }


    // 4133444|14278|6780|1|12|14307.24|0.01|0.06|R|F|1993-03-03|1993-03-22|1993-03-22|TAKE BACK RETURN|FOB|ular foxes cajole. instruct|
    public void map(LongWritable key, Text value, OutputCollector<LongWritable, Text> output, Reporter reporter) throws IOException {
      String token = "TAKE BACK RETURN";

      String[] values = value.toString().split("\\|");
      valueOfKey.set(Integer.valueOf(values[0].trim()));

      // find token in string
      if (value.find(token) >= 0) {
        output.collect(valueOfKey, value);
      }
    }
  }

  public static class FilterReducer implements Reducer<LongWritable, Text, LongWritable, Text> {

    @Override
    public void close() throws IOException {
    }

    @Override
    public void configure(JobConf jobConf) {
    }

    @Override
    public void reduce(LongWritable key, Iterator<Text> iterator, OutputCollector<LongWritable, Text> output, Reporter reporter) throws IOException {
      while (iterator.hasNext()) {
        output.collect(key, iterator.next());
      }
    }
  }

  /**
   * Main
   */
  public static void main(String[] args) throws Exception {
    JobConf conf = new JobConf();
    conf.setJobName("Filter");

    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length != 2) {
      System.err.println("Usage: hadoop jar mrexamples.jar br.ufpr.inf.lbd.examples.Filter <in> <out>");
      System.exit(2);
    }

    conf.setJarByClass(Aggregation.class);
    conf.setInputFormat(TextInputFormat.class);
    conf.setOutputFormat(TextOutputFormat.class);

    conf.setMapperClass(FilterMapper.class);
    conf.setReducerClass(FilterReducer.class);

    conf.setMapOutputKeyClass(LongWritable.class);
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