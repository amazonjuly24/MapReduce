import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class maxlen {

  public static class TokenizerMapper
       extends Mapper<LongWritable, Text, Text, IntWritable>{

    //private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(LongWritable key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());
      while (itr.hasMoreTokens()) {
        value.set(itr.nextToken());
        context.write(value, new IntWritable(value.toString().length()));
      }
    }
  
}

  public static class IntSumReducer
       extends Reducer<Text,IntWritable,Text,IntWritable> {
   // private IntWritable result = new IntWritable();
String max=new String();
    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
if(key.toString().length()>max.length())
{
max=key.toString();
}      
}
protected void cleanup(Context context) throws IOException, InterruptedException {

context.write(new Text(max), new IntWritable(max.length()));


     // result.set(sum);
     // context.write(new Text(max), new IntWritable(max.length()));
    }
  
}
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "maxlen");
    job.setJarByClass(maxlen.class);
    job.setMapperClass(TokenizerMapper.class);
    //job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
