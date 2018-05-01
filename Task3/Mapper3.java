import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.util.*;


public class Mapper3
  extends Mapper<LongWritable, Text, IntWritable, IntWritable> 
{
	 private final static IntWritable one = new IntWritable(1);	
	 private final static IntWritable zero = new IntWritable();
	 IntWritable t1;
	 IntWritable t2;
	 public void setup(Context context)
	 {
		 t1 = new IntWritable();
		 t2 = new IntWritable();
	 }
	public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException 
{   
	String[] lineArray = value.toString().split("\\|");
	if(Integer.parseInt(lineArray[2])==0)
	{
		t1.set(Integer.parseInt(lineArray[1]));
	    context.write(t1, one); 
	}
		else
		{
			t2.set(Integer.parseInt(lineArray[1]));
		    context.write(t2, zero); 
		}
	}
}
	