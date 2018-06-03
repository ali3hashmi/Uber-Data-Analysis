//Find the days on which each basement has more trips

import java.io.IOException;
import java.text.ParseException;
import java.util.Date;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class UberData1 {
	
	public static class TokenizerMapper extends Mapper<Object,Text,Text,IntWritable>
	{
		java.text.SimpleDateFormat format = new java.text.SimpleDateFormat("mm/dd/yyy");
		String[] days ={"sun","mon","tue","wed","thu","fri","sat"};
		private Text basement = new Text();
		Date date =null;
		private int trips;
		public void map(Object key,Text value,Context context) throws IOException,InterruptedException
		{
			String line = value .toString();
			String[] splits = line.split(",");
			basement.set(splits[0]);
			try{
				date =format.parse(splits[1]);
			}catch (ParseException e){
				e.printStackTrace();
			}
			trips= new Integer(splits[3]);
			String keys = basement.toString()+" "+days[date.getDay()];
			context.write(new Text(keys),new IntWritable(trips));
		}
	}
	
	public static class SumReducer extends Reducer<Text,IntWritable,Text,IntWritable>
	{
		private IntWritable result = new IntWritable();
		
		public void reduce(Text key,Iterable<IntWritable> values,Context context) throws IOException,InterruptedException
		{
			int sum =0;
			for(IntWritable val: values)
			{
				sum += val.get();
			}
			
			result.set(sum);
			context.write(key, result);
		}
	}


	public static void main(String args[]) throws Exception
	{
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf,"UberData1");
		job.setJarByClass(UberData1.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(SumReducer.class);
		job.setReducerClass(SumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job,new Path(args[0]));
		FileOutputFormat.setOutputPath(job,new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		
	}
	
}
