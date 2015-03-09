import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Counter extends Configured implements Tool {
	public int run(String[] args) throws Exception {
		
		String outputDir = "hbase_a1";
		
		Configuration conf = HBaseConfiguration.create(getConf());

		conf.addResource(new Path(
				"/users/level4/1101651g/workspace/eclipse/bd4-hbase/conf/core-site.xml"));
		conf.set("mapred.jar",
				"/users/level4/1101651g/workspace/eclipse/bd4-hbase/test.jar");
		Job job = new Job(conf);
		job.setJarByClass(Counter.class);
		Scan scan = new Scan();
		scan.setCaching(500);
		scan.setCacheBlocks(false);
		// Always set this to false for MR jobs!

		TableMapReduceUtil.initTableMapperJob("BD4Project2Sample", scan, Map.class, LongWritable.class, LongWritable.class, job);
		//job.setCombinerClass(Combiner.class);

		job.setNumReduceTasks(1);
		job.setReducerClass(Reduce.class);
		
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileOutputFormat.setOutputPath(job, new Path("hdfs://bigdata-01.dcs.gla.ac.uk:8020/user/1101651g/" + outputDir));
		
		job.submit();
		boolean status = job.waitForCompletion(true);

		return status ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Configuration(), new Counter(), args);

		System.exit(0);
	}

}