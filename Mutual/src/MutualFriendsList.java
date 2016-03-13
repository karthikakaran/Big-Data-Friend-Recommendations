import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class MutualFriendsList{
	enum MYCOUNTER {
		RECORD_COUNT
	}
	public static class Map extends Mapper<LongWritable, Text, Text, Text>{
		private Text userIds = new Text();
		private Text mutualFrList = new Text();
		Set<String> user1L, user2L;
		HashMap<String,String> myMap;

		String userA = "", userB = "";
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			String val[] = value.toString().split("\\t");
			if(val[0].equals(userA) || val[0].equals(userB)){
				String chain ="";
				if(myMap.get(userA) != null){
					List<String> user1List = Arrays.asList( myMap.get(userA).split(",") );
					user1L = new HashSet<String>(user1List);
				}
				if(myMap.get(userB) != null){
					List<String> user2List = Arrays.asList( myMap.get(userB).split(",") );
					user2L = new HashSet<String>(user2List);
				}
				if(user1L != null && user2L != null){
					user1L.retainAll(user2L);
					for(String uFd : user1L){
						chain = chain+","+uFd;
					}

					userIds.set(userA+","+userB+" ");
					mutualFrList.set(chain.substring(1));
					if(context.getCounter(MYCOUNTER.RECORD_COUNT).getValue() == 0){
						context.write(userIds,mutualFrList);
						context.getCounter(MYCOUNTER.RECORD_COUNT).increment(1);
					}
				}
			}
		}

		@Override
		protected void setup(Context context)
				throws IOException, InterruptedException {
			super.setup(context);
			myMap = new HashMap<String,String>();

			Configuration conf = context.getConfiguration();
			String userName[] = conf.get("userAB").split(",");
			userA = userName[0]; 
			userB = userName[1];
			String inputPath = conf.get("inpPath").toString();
			Path part=new Path("hdfs://cshadoop1"+inputPath);

			FileSystem fs = FileSystem.get(conf);
			FileStatus[] fss = fs.listStatus(part);

			for (FileStatus status : fss) {
				Path pt = status.getPath();

				BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
				String line;
				line=br.readLine();
				while (line != null){
					String[] userDetails = line.split("\\t");
					if(userDetails.length == 2){
						myMap.put(userDetails[0].trim(), userDetails[1]); 
					}
					line=br.readLine();
				}

			}

		}	
	}
	// Driver program
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 4) {
			System.err.println("Usage: MutualFriendsList <ip file> <out file> <userA> <userB>");
			System.exit(2);
		}

		conf.set("userAB", otherArgs[2]+","+otherArgs[3]);
		conf.set("inpPath", otherArgs[0]);
		Job job = new Job(conf, "MutualFriendsList");
		job.setJarByClass(MutualFriendsList.class);

		job.setMapperClass(Map.class);
		job.setNumReduceTasks(0);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileSystem outFs = new Path(otherArgs[1]).getFileSystem(conf);
		outFs.delete(new Path(otherArgs[1]), true);

		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}