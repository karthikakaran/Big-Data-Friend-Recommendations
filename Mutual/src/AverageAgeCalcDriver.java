import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class AverageAgeCalcDriver extends Configured implements Tool {
	
	private static final String OUTPUT_PATH = "intermediate_output";
	@Override
	public int run(String[] args) throws Exception {
		if (args.length != 3) {
			System.out.printf("Usage: AverageAgeCalcDriver- <input dir> <dist cache file> <output dir>");
			return -1;
		}
		/**JOB 1****/
		Job job = new Job(getConf());
		Configuration conf = job.getConfiguration();
		conf.set("cachePath", args[1].substring(args[1].lastIndexOf("/")+1));
		job.setJobName("Reduce-side join with text lookup file in DCache");
		DistributedCache.addCacheFile(new URI("hdfs://cshadoop1"+args[1]),conf);

		FileSystem outFs1 = new Path(OUTPUT_PATH).getFileSystem(conf);
		outFs1.delete(new Path(OUTPUT_PATH), true);

		job.setJarByClass(AverageAgeCalcDriver.class);
		job.setOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(AverageAgeCalcMapper.class);
		job.setReducerClass(AverageAgeCalcReducer.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		TextInputFormat.addInputPath(job, new Path(args[0]));
		TextOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH));

		job.waitForCompletion(true);

		/**JOB 2****/
		Job job2 = new Job(getConf());
		Configuration conf2 = job2.getConfiguration();
		conf2.set("cachePath", args[1].substring(args[1].lastIndexOf("/")+1));
		job2.setJobName("Job Chaining side join with text lookup file in DCache");
		DistributedCache.addCacheFile(new URI("hdfs://cshadoop1"+args[1]),conf2);
		
		FileSystem outFs = new Path(args[2]).getFileSystem(conf2);
		outFs.delete(new Path(args[2]), true);
		
		job2.setJarByClass(AverageAgeCalcDriver.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		job2.setMapperClass(AddressMapper.class);

		job2.setInputFormatClass(TextInputFormat.class);
		job2.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(job2, new Path(OUTPUT_PATH));
		FileOutputFormat.setOutputPath(job2, new Path(args[2]));

		boolean success = job2.waitForCompletion(true);
		return success ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new Configuration(),new AverageAgeCalcDriver(), args);
		System.exit(exitCode);
	}
}