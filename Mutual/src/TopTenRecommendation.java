import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class TopTenRecommendation {
	static public class CountMutualFriends implements Writable {
		public Long user;
		public Long mutualUserFriend;

		public CountMutualFriends(Long user, Long mutualFriend) {
			this.user = user;
			this.mutualUserFriend = mutualFriend;
		}

		public CountMutualFriends() {
			this(-1L, -1L);
		}

		@Override
		public void write(DataOutput out) throws IOException {
			out.writeLong(user);
			out.writeLong(mutualUserFriend);
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			user = in.readLong();
			mutualUserFriend = in.readLong();
		}

		@Override
		public String toString() {
			return " toUser: "
					+ Long.toString(user) + " mutualUserFriend: " + Long.toString(mutualUserFriend);
		}
	}

	public static class Map extends Mapper<LongWritable, Text, LongWritable, CountMutualFriends> {
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line[] = value.toString().split("\t");
			Long mainUser = Long.parseLong(line[0]);
			List<Long> userInList = new ArrayList<Long>();

			if (line.length == 2) {
				StringTokenizer tokenizer = new StringTokenizer(line[1], ",");
				while (tokenizer.hasMoreTokens()) {
					Long toUser = Long.parseLong(tokenizer.nextToken());
					userInList.add(toUser);
					context.write(new LongWritable(mainUser), new CountMutualFriends(toUser, -1L));
				}

				for (int i = 0; i < userInList.size(); i++) {
					for (int j = i + 1; j < userInList.size(); j++) {
						context.write(new LongWritable(userInList.get(i)), new CountMutualFriends((userInList.get(j)), mainUser));
						context.write(new LongWritable(userInList.get(j)), new CountMutualFriends((userInList.get(i)), mainUser));
					}
				}
			}
		}
	}

	public static class Reduce extends Reducer<LongWritable, CountMutualFriends, LongWritable, Text> {
		public static HashMap<Long, Text> countMap  = new HashMap<Long, Text>();
		@Override
		public void reduce(LongWritable key, Iterable<CountMutualFriends> values, Context context)
				throws IOException, InterruptedException {

			final java.util.Map<Long, List<Long>> mutualList = new HashMap<Long, List<Long>>();
			for (CountMutualFriends val : values) {
				final Boolean isAlreadyFriend = (val.mutualUserFriend == -1);
				final Long toUser = val.user;
				final Long mutualFriend = val.mutualUserFriend;

				if (mutualList.containsKey(toUser)) {
					if (isAlreadyFriend) {
						mutualList.put(toUser, null);
					} else if (mutualList.get(toUser) != null) {
						mutualList.get(toUser).add(mutualFriend);
					}
				} else {
					if (!isAlreadyFriend) {
						mutualList.put(toUser, new ArrayList<Long>() {
							{
								add(mutualFriend);
							}
						});
					} else {
						mutualList.put(toUser, null);
					}
				}
			}
			java.util.SortedMap<Long, List<Long>> mutualFriendsSort = new TreeMap<Long, List<Long>>();
			for (java.util.Map.Entry<Long, List<Long>> entry : mutualList.entrySet()) {
				if (entry.getValue() != null) {
					mutualFriendsSort.put(entry.getKey(), entry.getValue());	
				}
			}

			Integer i = 0;
			String output = "";
			for (java.util.Map.Entry<Long, List<Long>> entry : mutualFriendsSort.entrySet()) {
				if (i == 0) {
					output = entry.getKey().toString();
				} else
					output += "," + entry.getKey().toString();
				++i;
			}
			countMap.put(Long.parseLong(key.toString()), new Text(output));
		}
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			HashMap<Long, Text> sortedMap = (HashMap<Long, Text>) SortTopTen.sortByValues(countMap);
			for (Long key : sortedMap.keySet()) {
				String output = "";
				String tempArr[] = sortedMap.get(key).toString().split(",");
				if(tempArr.length > 10){

					for(int i=0;i<10;i++){
						if (i == 0) output = tempArr[i];
						else output += "," + tempArr[i];
					}
				} 
				else
					output =  sortedMap.get(key).toString();
				context.write(new LongWritable(key), new Text(output));
			}
		}
	}

	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.println("Usage: TopTenRecommendations <ip file> <out file>");
			System.exit(2);
		}
		Configuration conf = new Configuration();

		Job job = new Job(conf, "TopTenRecommendation");
		job.setJarByClass(TopTenRecommendation.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(CountMutualFriends.class);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileSystem outFs = new Path(args[1]).getFileSystem(conf);
		outFs.delete(new Path(args[1]), true);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
	}
}
class SortTopTen{
	static <K extends Comparable, V extends Comparable> Map<K, V> sortByValues(Map<K, V> map) {
		List<Map.Entry<K, V>> entries = new LinkedList<Map.Entry<K, V>>(map.entrySet());
		Collections.sort(entries, new Comparator<Map.Entry<K, V>>() {
			public int compare(Map.Entry<K, V> o1, Map.Entry<K, V> o2) {
				if (o2.getValue().toString().length() < o1.getValue().toString().length())
					return -1;
				else if (o2.getValue().toString().length() == o1.getValue().toString().length() 
						&& Long.parseLong(o1.getKey().toString()) < Long.parseLong(o2.getKey().toString()))
					return -1;
				else
					return 1;
			}
		});
		Map<K, V> sortedMap = new LinkedHashMap<K, V>();
		for (Map.Entry<K, V> entry : entries) {
			sortedMap.put(entry.getKey(), entry.getValue());
		}
		return sortedMap;
	}
}