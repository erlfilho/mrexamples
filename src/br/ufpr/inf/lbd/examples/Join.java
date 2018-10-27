package br.ufpr.inf.lbd.examples;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.StringTokenizer;

/**
 * Reduce Side join
 * select * from lineitem l join orders o on o.o_orderkey = l.l_orderkey;
 */
public class Join {

    public static class ReduceSideJoinPartitioner implements Partitioner<Text, Text> {
        @Override
        public void configure(JobConf jobConf) {
        }

        @Override
        public int getPartition(Text key, Text text, int numPartitions) {
            return key.hashCode() % numPartitions;
        }
    }

    public static class ReduceSideKeyComparator extends WritableComparator {
        public ReduceSideKeyComparator() {
            super(Text.class,true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            Text key1 = (Text)a;
            Text key2 = (Text)b;
            return key1.compareTo(key2);
        }
    }

    public static class ReduceSideJoinMapper implements Mapper<LongWritable, Text, Text, Text> {

        private int keyIndex; // the position of the index in the line
        private String delimiter;
        private Text joinKey = new Text();
        private Text joinAttributes = new Text();
        // private int joinOrder;

        @Override
        public void configure(JobConf jobConf) {
            keyIndex = Integer.parseInt(jobConf.get("keyIndex"));
            delimiter = jobConf.get("delimiter");
        }

        @Override
        public void close() throws IOException {

        }

        @Override
        public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {

            // split line by delimiter to an array
            StringTokenizer tokenizer = new StringTokenizer(value.toString(), delimiter);
            ArrayList<String> values = new ArrayList<>();
            while (tokenizer.hasMoreTokens()) {
                values.add(tokenizer.nextToken());
            }

            // get key and remove it from array
            String joinKey = values.remove(keyIndex);

            // rebuild the tuple without key
            StringBuilder builder = new StringBuilder();
            for (int i=0; i<values.size()-1; i++) {
                builder.append(values.get(i)).append(",");
            }
            builder.append(values.get(values.size()-1));

            this.joinKey.set(joinKey);
            this.joinAttributes.set(builder.toString());
            output.collect(this.joinKey, joinAttributes);
        }

    }

    public static class ReduceSideJoinReducer implements Reducer<Text, Text, LongWritable, Text> {

        private Text joinedText = new Text();
        private StringBuilder builder = new StringBuilder();
        private LongWritable keyLongWritable = new LongWritable();


        @Override
        public void close() throws IOException {
        }

        @Override
        public void configure(JobConf jobConf) {
        }

        @Override
        public void reduce(Text key, Iterator<Text> values, OutputCollector<LongWritable, Text> output, Reporter reporter) throws IOException {
            // reconstruct tuple with key
            builder.append(key).append("|");
            while (values.hasNext()) {
                builder.append(values.next().toString()).append("|");
            }
            builder.setLength(builder.length()-1);
            joinedText.set(builder.toString());

            // set key from string to longWritable
            keyLongWritable.set(Long.valueOf(String.valueOf(key)));

            // write <key, tuple>
            output.collect(keyLongWritable, joinedText);
            builder.setLength(0);
        }

    }

    public static void main(String[] args) throws Exception {
        JobConf conf = new JobConf();
        conf.setJobName("Join");
        StringBuilder filePaths = new StringBuilder();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        if (args.length != 3) {
            System.err.println("Usage: hadoop jar mrexamples.jar br.ufpr.inf.lbd.examples.Join <table1> <table2> <output dir>");
            System.exit(2);
        }

        conf.set("keyIndex", "0");
        conf.set("delimiter", "|");

        for(int i = 0; i< args.length - 1; i++) {
            // get file name, remove path
            String[] path = otherArgs[i].split("/");
            String fileName = path[path.length-1];

            conf.set(fileName, Integer.toString(i+1));
            filePaths.append(args[i]).append(",");
        }

        filePaths.setLength(filePaths.length() - 1);

        // set input and output path
        FileInputFormat.addInputPath(conf, new Path(args[0]));
        FileInputFormat.addInputPath(conf, new Path(args[1]));
        FileOutputFormat.setOutputPath(conf, new Path(args[2]));

        // configure job
        conf.setNumReduceTasks(11);
        conf.setJarByClass(Join.class);
        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);
        conf.setMapperClass(ReduceSideJoinMapper.class);
        conf.setReducerClass(ReduceSideJoinReducer.class);
        conf.setMapOutputKeyClass(Text.class);
        conf.setMapOutputValueClass(Text.class);
        conf.setOutputKeyClass(LongWritable.class);
        conf.setOutputValueClass(Text.class);

        // run it
        JobClient jc = new JobClient(conf);
        RunningJob job = jc.submitJob(conf);
        jc.monitorAndPrintJob(conf, job);
    }

}
