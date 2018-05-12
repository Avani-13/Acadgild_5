import java.io.IOException;
import java.util.HashSet;
import java.util.Set;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;


public class Reducer1 extends
			Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

		public void reduce(
				IntWritable TrackId,
				Iterable<IntWritable> UserIds,
				Reducer<IntWritable, IntWritable, IntWritable, IntWritable>.Context context)
				throws IOException, InterruptedException {

			Set<Integer> UserIdSet = new HashSet<Integer>();
			for (IntWritable UserId : UserIds) {
				UserIdSet.add(UserId.get());
			}
			IntWritable size = new IntWritable(UserIdSet.size());
			context.write(TrackId, size);
		}
	}