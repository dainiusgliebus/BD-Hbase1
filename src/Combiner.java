import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;


public class Combiner extends TableReducer<ImmutableBytesWritable, ImmutableBytesWritable, ImmutableBytesWritable> {
	// <KEYIN,VALUEIN,KEYOUT>

 public void reduce(ImmutableBytesWritable key, Iterable<ImmutableBytesWritable> values, Context context) throws IOException, InterruptedException {
  int sum = 0;
  
  for (ImmutableBytesWritable v : values){
		sum++;
		//sum += v.get();
	}
  
  Put put = new Put(key.get());
  put.add(Bytes.toBytes("A"), key.get(), Bytes.toBytes(sum));
  
  context.write(key, put);
 }
}