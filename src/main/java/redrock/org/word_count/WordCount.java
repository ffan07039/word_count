package redrock.org.word_count;

import java.io.IOException;
import java.util.Iterator;
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

/**
 * Hello world!
 *
 */
public class WordCount 
{
    private static final IntWritable one = new IntWritable(1);
    private static final Text word = new Text();
	
    public static class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
		@Override
    	public void map(LongWritable key, Text value, Context context) throws InterruptedException, IOException{
			StringTokenizer st = new StringTokenizer(value.toString());
			while (st.hasMoreTokens()){
				word.set(st.nextToken());
				context.write(word, one);
			}
		}
	}
    
    public static class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    	@Override
    	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
    		int c = 0;
    		Iterator<IntWritable> it = values.iterator();
    		while (it.hasNext()){
    			c += it.next().get();
    		}
    		
    		context.write(key, new IntWritable(c));
    	}
    }
	
	public static void main( String[] args ) throws Exception
    {
	    Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf, "word count");
	    job.setJarByClass(WordCount.class);
	    job.setMapperClass(MyMapper.class);
	    job.setCombinerClass(MyReducer.class);
	    job.setReducerClass(MyReducer.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
    
    
}
