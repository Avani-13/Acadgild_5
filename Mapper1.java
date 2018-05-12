import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Mapper;

public class Mapper1 extends
Mapper< Object , Text, IntWritable, IntWritable > {
        int TrackId;
        int UserId;
 
public void map(Object key, Text value,
    Mapper< Object , Text, IntWritable, IntWritable > .Context context)
        throws IOException, InterruptedException {
 
    String[] parts = value.toString().split("\\|");
    IntWritable TrackId=new IntWritable(Integer.parseInt(parts[0]));
    IntWritable UserId=new IntWritable(Integer.parseInt(parts[1]));
        if (parts.length == 5) {
        context.write(TrackId, UserId);
    } else {
        // add counter for <span id="IL_AD12" class="IL_AD">invalid</span> records
        context.getCounter(Driver1.COUNTERS.INVALID_RECORD_COUNT).increment(1L);
    }
    }
}