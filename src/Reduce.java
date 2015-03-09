import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Reduce extends Reducer<LongWritable, Text, LongWritable, Text> {
	// <inputKey, inputValue, outKey, outValue

	private Text result = new Text();

	public void reduce(LongWritable key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		List<String> list = new ArrayList();

		int sum = 0;
		String tmp = "";

		for (Text val : values) {
			String array[] = val.toString().split(" ");
			
			for (int i = 0; i < array.length; i++) {
				list.add(array[i]);
				sum++;
			}
		}

		Collections.sort(list);

		for (String rev : list) {
			tmp += rev + " ";
		}

		result.set(sum + " " + tmp);

		context.write(key, result);

	}
}