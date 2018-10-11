package org.bigdata;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class NYSE_copy
{
	public static class TokenizerMapper extends Mapper<Object, Text, Text, B>
    {
		private Text word = new Text();
	    private final static IntWritable one = new IntWritable(1);
	    private final static IntWritable zero = new IntWritable(0);

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
        {
			String[] cols=value.toString().split(",");
	    	//String stock_symbol=cols[1];
	    	String date=cols[2];
	    	if(date.equals("date")==false && date.substring(0, 4).equals("2009")==true)
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
	    	
	    	B obj=new B(new IntWritable(res),new IntWritable(1));
	    	word.set(date);
	    	context.write(word, obj);
	    	}
       }
    
    }
	public static class IntSumReducer extends Reducer<Text,B,Text,B> 
	{
		public void reduce(Text key, Iterable<B> values,Context context) throws IOException, InterruptedException 
		{
			int sum=0,sum1=0;
		      for (B val : values) 
		      {
		        sum+=val.getmarks().get();
		        sum1+=val.getone().get();
		      }	
		      context.write(key, new B(new IntWritable(sum),new IntWritable(sum1)));
	    }
	}
	 public static class TokenizerMapper1 extends Mapper<Object, Text, Text, C>
	 {
		//private Text word = new Text();
		 public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
		 {
	    	String[] cols=value.toString().split("\t");
	    	String date=cols[0];
	    	int sum=Integer.parseInt(cols[1]);
	    	int sum1=Integer.parseInt(cols[2]);
			float res=(float) 0.0;
			res=((float)sum)/((float)sum1)*100;
			if(res>75.0)
			{
				
				context.write(new Text(date.substring(0, 7)),new C(new IntWritable(1),new IntWritable(0),new IntWritable(0),new IntWritable(0),new IntWritable(1)));
			}
			else if(res>50.0)
			{
				
				context.write(new Text(date.substring(0, 7)),new C(new IntWritable(0),new IntWritable(1),new IntWritable(0),new IntWritable(0),new IntWritable(1)));
			}
			else if(res>25.0)
			{
				
				context.write(new Text(date.substring(0, 7)),new C(new IntWritable(0),new IntWritable(0),new IntWritable(1),new IntWritable(0),new IntWritable(1)));
			}
	    	else
	    	{
	    		
	    		context.write(new Text(date.substring(0, 7)),new C(new IntWritable(0),new IntWritable(0),new IntWritable(0),new IntWritable(1),new IntWritable(1)));
	    	}
			
       }
    
    }
	
	public static class IntSumReducer1 extends Reducer<Text,C,Text,C> 
	{
		public void reduce(Text key, Iterable<C> values,Context context) throws IOException, InterruptedException 
		{
			int sumA=0,sumB=0,sumC=0,sumD=0,sum1=0;
		      for (C val : values) 
		      {
		        sumA+=val.getA().get();
		        sum1+=val.getone().get();
		        sumB+=val.getB().get();
		        sumC+=val.getC().get();
		        sumD+=val.getD().get();
		      }	
		      
		      context.write(key,new C(new IntWritable(sumA),new IntWritable(sumB),new IntWritable(sumC),new IntWritable(sumD),new IntWritable(sum1)));
	    }
	}
	public static class TokenizerMapper2 extends Mapper<Object, Text, Text, D>
	 {
		//private Text word = new Text();
		 public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
		 {
			D obj=new D();
			String[] cols=value.toString().split("\t");
	    	String date=cols[0];
	    	int A=Integer.parseInt(cols[1]);
	    	int B=Integer.parseInt(cols[2]);
	    	int C=Integer.parseInt(cols[3]);
	    	int D=Integer.parseInt(cols[4]);
	    	int one=Integer.parseInt(cols[5]);
	    	float resA=(float) 0.0,resB=(float) 0.0,resC=(float) 0.0,resD=(float) 0.0;
			resA=((float)A)/((float)one)*100;
			resB=((float)B)/((float)one)*100;
			resC=((float)C)/((float)one)*100;
			resD=((float)D)/((float)one)*100;
			obj=new D(new FloatWritable(resA),new FloatWritable(resB),new FloatWritable(resC),new FloatWritable(resD),new IntWritable(one));
			context.write(new Text(date),obj);
        }
   
    }
	
	public static class IntSumReducer2 extends Reducer<Text,D,Text,D> 
	{
		public void reduce(Text key, Iterable<D> values,Context context) throws IOException, InterruptedException 
		{
			for (D val : values) 
			{
	    		context.write(key, val);
	        }
	    }
	}
 public static void main(String[] args) throws Exception 
 {
	 Configuration conf = new Configuration();
	 Path out = new Path(args[1]);

	 Job job1 = Job.getInstance(conf, "word count");
	 job1.setJarByClass(NYSE_copy.class);
	 job1.setMapperClass(TokenizerMapper.class);
	 job1.setCombinerClass(IntSumReducer.class);
	 job1.setReducerClass(IntSumReducer.class);
	 job1.setMapOutputKeyClass(Text.class);
	 job1.setMapOutputValueClass(B.class);
	 job1.setOutputKeyClass(Text.class);
	 job1.setOutputValueClass(B.class);
	 FileInputFormat.addInputPath(job1, new Path(args[0]));
	 FileOutputFormat.setOutputPath(job1, new Path( args[1]));
	 if (!job1.waitForCompletion(true)) 
	 {
	   System.exit(1);
	 }
	 Job job2 = Job.getInstance(conf, "sort by frequency");
	 job2.setJarByClass(NYSE_copy.class);
	 job2.setMapperClass(TokenizerMapper1.class);
	 job2.setNumReduceTasks(1);
	 job2.setReducerClass(IntSumReducer1.class);
	 job2.setOutputKeyClass(Text.class);
	 job2.setOutputValueClass(C.class); 
	 FileInputFormat.addInputPath(job2, new Path( args[1]));
	 FileOutputFormat.setOutputPath(job2, new Path( args[2]));
	
		 if (!job2.waitForCompletion(true))
		 {
			 System.exit(1);
		 }   
	 Job job3 = Job.getInstance(conf, "sort by frequency");
	 job3.setJarByClass(NYSE_copy.class);
	 job3.setMapperClass(TokenizerMapper2.class);
	 job3.setNumReduceTasks(1);
	 
	 job3.setReducerClass(IntSumReducer2.class);
	 
	 job3.setOutputKeyClass(Text.class);
	 job3.setOutputValueClass(D.class);
	 
	 FileInputFormat.addInputPath(job3, new Path( args[2]));
	 FileOutputFormat.setOutputPath(job3, new Path( args[3]));
	
		 if (!job3.waitForCompletion(true))
		 {
			 System.exit(1);
		 }
   }
}
class B implements Writable
{
	IntWritable diff;
	IntWritable one;
	B()
	{
		diff=new IntWritable(0);
		one=new IntWritable(0);
	}
	B(IntWritable m, IntWritable o)
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
class C implements Writable
{
	IntWritable A,B,C,D;
	IntWritable one;
	C()
	{
		A=new IntWritable(0);
		B=new IntWritable(0);
		C=new IntWritable(0);
		D=new IntWritable(0);
		one=new IntWritable(0);
	}
	C(IntWritable a, IntWritable b, IntWritable c, IntWritable d, IntWritable ONE)
	{
		A=a;
		one=ONE;
		B=b;
		C=c;
		D=d;
	}
	IntWritable getA()
	{
		return A;
	}
	IntWritable getone()
	{
		return one;
	}
	IntWritable getB()
	{
		return B;
	}
	IntWritable getC()
	{
		return C;
	}
	IntWritable getD()
	{
		return D;
	}
	void setA (IntWritable A)
	{
		this.A=A;
	}
	void setone(IntWritable one)
	{
		this.one=one;
	}
	void setB (IntWritable B)
	{
		this.B=B;
	}
	void setC (IntWritable C)
	{
		this.C=C;
	}
	void setD (IntWritable D)
	{
		this.D=D;
	}
	  @Override
	   //overriding default readFields method. 
	   //It de-serializes the byte stream data
	   public void readFields(DataInput in) throws IOException 
	   {
		  A.readFields(in);
		  B.readFields(in);
		  C.readFields(in);
		  D.readFields(in);
		  one.readFields(in);
	    
	   }
	 
	   @Override
	   //It serializes object data into byte stream data
	   public void write(DataOutput out) throws IOException 
	   {
	    A.write(out);
	    B.write(out);
	    C.write(out);
	    D.write(out);
	    one.write(out);
	    
	    
	   }
	   @Override
       public String toString()
       {
           return A.toString()+"\t"+B.toString()+"\t"+C.toString()+"\t"+D.toString()+"\t"+one.toString();
       }
}
class D implements Writable
{
	FloatWritable A,B,C,D;
	IntWritable one;
	D()
	{
		A=new FloatWritable((float) 0.0);
		B=new FloatWritable((float) 0.0);
		C=new FloatWritable((float) 0.0);
		D=new FloatWritable((float) 0.0);
		one=new IntWritable(0);
	}
	D(FloatWritable a, FloatWritable b, FloatWritable c, FloatWritable d, IntWritable ONE)
	{
		A=a;
		B=b;
		C=c;
		D=d;
		one=ONE;
	}
	FloatWritable getA()
	{
		return A;
	}
	FloatWritable getB()
	{
		return B;
	}
	FloatWritable getC()
	{
		return C;
	}
	FloatWritable getD()
	{
		return D;
	}
	IntWritable getone()
	{
		return one;
	}
	void setA (FloatWritable A)
	{
		this.A=A;
	}
	void setB (FloatWritable B)
	{
		this.B=B;
	}
	void setC (FloatWritable C)
	{
		this.C=C;
	}
	void setD (FloatWritable D)
	{
		this.D=D;
	}
	void setA (IntWritable ONE)
	{
		this.one=ONE;
	}
	  @Override
	   //overriding default readFields method. 
	   //It de-serializes the byte stream data
	   public void readFields(DataInput in) throws IOException 
	   {
		  A.readFields(in);
		  B.readFields(in);
		  C.readFields(in);
		  D.readFields(in);
		  one.readFields(in);
	   }
	 
	   @Override
	   //It serializes object data into byte stream data
	   public void write(DataOutput out) throws IOException 
	   {
	    A.write(out);
	    B.write(out);
	    C.write(out);
	    D.write(out);
	    one.write(out);
	   }
	   @Override
       public String toString()
       {
           return A.toString()+"\t"+B.toString()+"\t"+C.toString()+"\t"+D.toString()+"\t"+one.toString();
       }
}