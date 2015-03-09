import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class Map extends
		TableMapper<LongWritable, LongWritable> {
	// <KEYOUT,VALUEOUT>
	private Text rev = new Text();
	private Date startDate = null;
	private Date endDate = null;
	private SimpleDateFormat dateFormat = new SimpleDateFormat(
			"yyyy-MM-dd'T'HH:mm:ss'Z'");

	public void map(ImmutableBytesWritable key, Result value, Context context)
			throws IOException, InterruptedException {

		try {
			// startDate =
			// dateFormat.parse(context.getConfiguration().getStrings("startDate")[0]);
			// endDate =
			// dateFormat.parse(context.getConfiguration().getStrings("endDate")[0]);
			startDate = dateFormat.parse("2007-01-01T11:22:33Z");
			endDate = dateFormat.parse("2008-01-01T11:22:33Z");
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		KeyValue[] res = value.raw();
		Date ts = new Date(res[0].getTimestamp());

		if (ts.after(startDate) && ts.before(endDate)) {
			byte[] article_id = Arrays.copyOfRange(key.get(), 0, 8);
			byte[] revision_id = Arrays.copyOfRange(key.get(), 8,
					key.getLength());

			context.write(new LongWritable(Bytes.toLong(article_id)),new LongWritable(Bytes.toLong(revision_id)));
		}
	}
}