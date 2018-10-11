package org.bigdata;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class NYSE 
{
	public static class TokenizerMapper extends Mapper<Object, Text, Text, A>
    {
		private Text word = new Text();
	    private final static IntWritable one = new IntWritable(1);
	    private final static IntWritable zero = new IntWritable(0);

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
        {
			String[] cols=value.toString().split(",");
	    	String stock_symbol=cols[1];
	    	String date=cols[2];
	    	if(stock_symbol.equals("stock_symbol")==false && date.substring(0, 4).equals("2009")==true)
	    	{
	    	
		    	float stock_price_open=Float.parseFloat(cols[3]);
		    	float stock_price_close=Float.parseFloat(cols[6]);
		    	float diff;
		    	int res=0;
	    		diff=stock_price_close-stock_price_open;
	    		if(diff>0)
	    		{
	    			res=1;
	    		}
	    		else
	    		{
	    			res=0;
	    		}
	    	
	    	A obj=new A(new IntWritable(res),new IntWritable(1));
	    	word.set(stock_symbol);
	    	context.write(word, obj);
	    	}
       }
    
    }
	public static class IntSumReducer extends Reducer<Text,A,Text,A> 
	{
		public void reduce(Text key, Iterable<A> values,Context context) throws IOException, InterruptedException 
		{
			int sum=0,sum1=0;
		      for (A val : values) 
		      {
		        sum+=val.getmarks().get();
		        sum1+=val.getone().get();
		      }	
		      context.write(key, new A(new IntWritable(sum),new IntWritable(sum1)));
	    }
	}
	 public static class TokenizerMapper1 extends Mapper<Object, Text, Text, IntWritable>
	 {
		//private Text word = new Text();
		 public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
		 {
	    	String[] cols=value.toString().split("\t");
	    	String stock_symbol=cols[0];
	    	int sum=Integer.parseInt(cols[1]);
	    	int sum1=Integer.parseInt(cols[2]);
			float res=(float) 0.0;
			res=((float)sum)/((float)sum1)*100;
			if(res>67.0)
			{
			    context.write(new Text("A"), new IntWritable(1));
			}
			else if(res>40.0)
			{
			    context.write(new Text("B"), new IntWritable(1));
			}
	    	else
	    	{
	    		context.write(new Text("C"), new IntWritable(1));
	    	}
       }
    
    }
	
	public static class IntSumReducer1 extends Reducer<Text,IntWritable,Text,IntWritable> 
	{
		public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException 
		{
			int sum=0;
		      for (IntWritable val : values) 
		      {
		        sum+=val.get();
		      }	
		      context.write(key,(new IntWritable(sum)));
	    }
	}
 public static void main(String[] args) throws Exception 
 {
	 Configuration conf = new Configuration();
	 Path out = new Path(args[1]);

	 Job job1 = Job.getInstance(conf, "word count");
	 job1.setJarByClass(NYSE.class);
	 job1.setMapperClass(TokenizerMapper.class);
	 job1.setCombinerClass(IntSumReducer.class);
	 job1.setReducerClass(IntSumReducer.class);
	 job1.setMapOutputKeyClass(Text.class);
	 job1.setMapOutputValueClass(A.class);
	 job1.setOutputKeyClass(Text.class);
	 job1.setOutputValueClass(A.class);
	 FileInputFormat.addInputPath(job1, new Path(args[0]));
	 FileOutputFormat.setOutputPath(job1, new Path( args[1]));
	 if (!job1.waitForCompletion(true)) 
	 {
	   System.exit(1);
	 }
	 Job job2 = Job.getInstance(conf, "sort by frequency");
	 job2.setJarByClass(NYSE.class);
	 job2.setMapperClass(TokenizerMapper1.class);
	 job2.setNumReduceTasks(1);
	 //job1.setCombinerClass(IntSumReducer1.class);
	 job2.setReducerClass(IntSumReducer1.class);
	 //job2.setSortComparatorClass(LongWritable.DecreasingComparator.class);
	 job2.setOutputKeyClass(Text.class);
	 job2.setOutputValueClass(IntWritable.class);
	 //job2.setInputFormatClass(SequenceFileInputFormat.class);
	 FileInputFormat.addInputPath(job2, new Path( args[1]));
	 FileOutputFormat.setOutputPath(job2, new Path( args[2]));
	
		 if (!job2.waitForCompletion(true))
		 {
			 System.exit(1);
		 }   
	
   }
}
class A implements Writable
{
	IntWritable diff;
	IntWritable one;
	A()
	{
		diff=new IntWritable(0);
		one=new IntWritable(0);
	}
	A(IntWritable m, IntWritable o)
	{
		diff=m;
		one=o;
	}
	IntWritable getmarks()
	{
		return diff;
	}
	IntWritable getone()
	{
		return one;
	}
	void setmarks (IntWritable diff)
	{
		this.diff=diff;
	}
	void setone(IntWritable one)
	{
		this.one=one;
	}
	  @Override
	   //overriding default readFields method. 
	   //It de-serializes the byte stream data
	   public void readFields(DataInput in) throws IOException 
	   {
		  diff.readFields(in);
		  one.readFields(in);
	    
	   }
	 
	   @Override
	   //It serializes object data into byte stream data
	   public void write(DataOutput out) throws IOException 
	   {
	    diff.write(out);
	    one.write(out);
	    
	    
	   }
	   @Override
       public String toString()
       {
           return diff.toString()+"\t"+one.toString();
       }
}
