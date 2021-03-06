import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
//import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class Reducer2
  extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
  
  @Override
  
  public void reduce(IntWritable key, Iterable<IntWritable> values,
      Context context)
      throws IOException, InterruptedException {
      System.out.println("TrackId=>"+key) ;	
    
      int sum = 0;
      for (IntWritable value : values) {
    	  sum+=value.get();	
       }		
       context.write(key, new IntWritable(sum));
  }
}
