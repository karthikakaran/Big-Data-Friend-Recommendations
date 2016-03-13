import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapSideMutualClass extends Mapper<LongWritable, Text, Text, Text> {

	private static HashMap<String, String> friendMap = new HashMap<String, String>();
	private BufferedReader brReader;
	ArrayList<String> detaild = new ArrayList<String>();
	Set<String> user1L, user2L;

	private Text txtMapOutputKey = new Text("");
	private Text txtMapOutputValue = new Text("");
	private String userA = "", userB = "";
	enum MYCOUNTER {
		RECORD_COUNT, FILE_EXISTS, FILE_NOT_FOUND, SOME_OTHER_ERROR
	}

	@Override
	protected void setup(Context context) throws IOException,InterruptedException {
		Configuration conf = context.getConfiguration();
		String userName[] = conf.get("userAB").split(",");
		userA = userName[0]; 
		userB = userName[1];
		String cachePath = conf.get("cachePath").toString();
		Path[] cacheFilesLocal = DistributedCache.getLocalCacheFiles(context.getConfiguration());

		for (Path eachPath : cacheFilesLocal) {
			if (eachPath.getName().toString().trim().equals(cachePath)) {
				context.getCounter(MYCOUNTER.FILE_EXISTS).increment(1);
				loadUserDetailsMap(eachPath, context);
				if(friendMap.get(userA) != null){
					List<String> user1List= Arrays.asList( friendMap.get(userA).split(",") );
					user1L = new HashSet<String>(user1List);
				}
				if(friendMap.get(userB) != null){
					List<String> user2List = Arrays.asList( friendMap.get(userB).split(",") );
					user2L = new HashSet<String>(user2List);
				}
				if(user1L != null && user2L != null)
					user1L.retainAll(user2L);
				else if(user1L == null || user2L == null)
					user1L = null;
			}
		}	
	}

	private void loadUserDetailsMap(Path filePath, Context context) throws IOException {
		String line = "";
		try {
			brReader = new BufferedReader(new FileReader(filePath.toString()));
			while ((line = brReader.readLine()) != null) {
				String[] userDetails = line.split("\\t");
				if(userDetails.length == 2){
					friendMap.put(userDetails[0].trim(), userDetails[1]); 
				}
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			context.getCounter(MYCOUNTER.FILE_NOT_FOUND).increment(1);
		} catch (IOException e) {
			context.getCounter(MYCOUNTER.SOME_OTHER_ERROR).increment(1);
			e.printStackTrace();
		}finally {
			if (brReader != null) {
				brReader.close();
			}
		}
	}

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String name = "", zipCode = "";
		context.getCounter(MYCOUNTER.RECORD_COUNT).increment(1);
		String userDetails[] = value.toString().split(",");
		if(user1L != null){
			if (user1L.contains(userDetails[0])) {
				try {
					name = userDetails[1];
					zipCode = userDetails[6];
					detaild.add(name+":"+zipCode);

				} catch(Exception e){
				}			
				txtMapOutputKey.set(userA+" "+userB+" ");
				txtMapOutputValue.set(detaild.toString());

				user1L.remove(userDetails[0]);
				if(user1L.size() == 0)
					context.write(txtMapOutputKey, txtMapOutputValue);
			}
		}
	}
}