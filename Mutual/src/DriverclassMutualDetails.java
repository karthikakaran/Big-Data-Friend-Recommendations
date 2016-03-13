import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class DriverclassMutualDetails extends Configured implements Tool {
	@Override
	public int run(String[] args) throws Exception {

		if (args.length != 5) {
			System.err.println("Usage: DriverclassMutualDetails <ip file> <dist cache file> <out file> <userA> <userB>");
			System.exit(2);
		}

		Job job = new Job(getConf());
		Configuration conf = job.getConfiguration();
		conf.set("userAB", args[3]+","+args[4]);
		conf.set("cachePath", args[1].substring(args[1].lastIndexOf("/")+1));

		job.setJobName("Map-side join with text lookup file in DCache");
		DistributedCache.addCacheFile(new URI("hdfs://cshadoop1"+args[1]),conf);

		job.setJarByClass(DriverclassMutualDetails.class);

		FileSystem outFs = new Path(args[2]).getFileSystem(conf);
		outFs.delete(new Path(args[2]), true);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		job.setMapperClass(MapSideMutualClass.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		boolean success = job.waitForCompletion(true);
		return success ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new Configuration(), new DriverclassMutualDetails(), args);
		System.exit(exitCode);
	}
}