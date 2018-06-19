import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount {

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, NullWritable>{

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
//private Text removeWord = new Text ("#");
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());
while (itr.hasMoreTokens()) {
String token = itr.nextToken();
if(token.contains(removeWord.toString())) {}
else{

word.set(token.toLowerCase());
context.write(word, NullWritable.get());

}
    }
  }

}

  public static class IntSumReducer
       extends Reducer<Text,NullWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }

 

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(WordCount.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(NullWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}


















-------------------
public static void main(String args[])throws Exception
    {
      //System.out.print(calgcd()+" ");  
      BigInteger sum=BigInteger.ZERO;
         BigInteger x=BigInteger.ONE;
        long limit=100000000000l;
        //BigInteger limit=BigInteger.TEN.pow(11);
        for(long i=1;i<=10;i++)
        {
            
            for(long j=1;j<=i;j++)
            {
                
                x=BigInteger.valueOf(i).gcd(BigInteger.valueOf(j));
                
                    //sum=sum.add(x);
                
                
               //System.out.print("\t"+x+"  ");
            }
            
        }
        System.out.print(" \n"+sum+" ");
    }
 


