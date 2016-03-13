import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AddressMapper extends Mapper  <LongWritable, Text, Text, Text>{

	private static HashMap<String, String> userAddressMap = new HashMap<String, String>();
	private BufferedReader brReader;
	enum MYCOUNTER {
		RECORD_COUNT, FILE_EXISTS, FILE_NOT_FOUND, SOME_OTHER_ERROR
	}
	@Override
	protected void setup(Context context) throws IOException,InterruptedException {
		Configuration conf = context.getConfiguration();
		String cachePath = conf.get("cachePath").toString();
		Path[] cacheFilesLocal = DistributedCache.getLocalCacheFiles(context.getConfiguration());
		for (Path eachPath : cacheFilesLocal) {
			if (eachPath.getName().toString().trim().equals(cachePath)) {
				context.getCounter(MYCOUNTER.FILE_EXISTS).increment(1);
				loadUserAddress(eachPath, context);
			}
		}
	}
	private void loadUserAddress(Path filePath, Context context) throws IOException
	{
		String line = "";
		try {
			brReader = new BufferedReader(new FileReader(filePath.toString()));
			while ((line = brReader.readLine()) != null) {
				String userId = line.substring(0,line.indexOf(','));
				String userDetails = line.substring(line.indexOf(',')+1);
				if(userDetails.length() > 1 && userId.length() > 1){
					userAddressMap.put(userId.trim(), userDetails); 
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
		Text outputText = new Text();
		String address = "";
		String avgFriendsAge[] = value.toString().split("\t");
		if (userAddressMap.containsKey(avgFriendsAge[0])) {
			try {
				String details[] = userAddressMap.get(avgFriendsAge[0]).split(",");
				address = avgFriendsAge[0]+","+details[2]+","+details[3]+","+details[4]+","+avgFriendsAge[1];
				outputText.set(address);
			}catch(Exception e){
				outputText.set(e.getMessage());
			} 		
		}
		context.write(outputText, new Text(""));
	}
}
