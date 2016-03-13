import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Date;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AverageAgeCalcReducer extends Reducer<Text, Text, Text, Text>
{
	static Map<String, Float> countMap = new HashMap<String, Float>();
	private BufferedReader brReader;
	enum MYCOUNTER {
		RECORD_COUNT, FILE_EXISTS, FILE_NOT_FOUND, SOME_OTHER_ERROR
	}
	private static HashMap<String,String> userDetailsMap= new HashMap<String,String>();
	private static int count = 0;
	@Override
	protected void setup(Context context) throws IOException,InterruptedException {
		Configuration conf = context.getConfiguration();
		String cachePath = conf.get("cachePath").toString();
		Path[] cacheFilesLocal = DistributedCache.getLocalCacheFiles(context.getConfiguration());

		for (Path eachPath : cacheFilesLocal) {
			if (eachPath.getName().toString().trim().equals(cachePath)) {
				context.getCounter(MYCOUNTER.FILE_EXISTS).increment(1);
				loadDeliveryStatusCodes(eachPath, context);
			}
		}

	}

	private void loadDeliveryStatusCodes(Path filePath, Context context) throws IOException
	{
		String line = "";
		try {
			brReader = new BufferedReader(new FileReader(filePath.toString()));
			while ((line = brReader.readLine()) != null) {
				String userId = line.substring(0,line.indexOf(','));
				String userDetails = line.substring(line.indexOf(',')+1);
				if(userDetails.length() > 1 && userId.length() > 1){
					userDetailsMap.put(userId.trim(), userDetails); 
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
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		final java.util.Map<Long,  Float> avgAgeList = new HashMap<Long,  Float>();
		for (Text value : values)
		{
			String userFriendList = value.toString();
			float averageAge = 0.0f;
			if(userFriendList != null){
				String userListSplitted[] = userFriendList.split(",");
				float age = 0.0f;
				Calendar cal = Calendar.getInstance();	
				float year = cal.get(Calendar.YEAR);
				float month = cal.get(Calendar.MONTH) + 1;
				float date = cal.get(Calendar.DATE);
				
				for (String userID : userListSplitted) {
					if(userDetailsMap.get(userID) != null){
						float yearDiff = 0.0f, monthDiff = 0.0f, dayDiff = 0.0f;
						String dobArr[] = userDetailsMap.get(userID).toString().split(",");
						String dob[] = dobArr[8].split("/");
						yearDiff = year - Integer.parseInt(dob[2]);
						monthDiff = month - Integer.parseInt(dob[0]);
						dayDiff = date - Integer.parseInt(dob[1]);
						age += (float)yearDiff + (float)(monthDiff/12) + (float)(dayDiff/365);
					}
				}
				averageAge = (float)(age/userListSplitted.length);
				avgAgeList.put(Long.parseLong(key.toString()), averageAge);
			}	
		}
		for (java.util.Map.Entry<Long,  Float> entry : avgAgeList.entrySet()) {
			if (entry.getValue() != null) {
				countMap.put(entry.getKey().toString(), entry.getValue());
			}
		}
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		LinkedHashMap sortedMap = sortHashMapByValuesD(countMap);
		List<Entry<String, Float>> list = new ArrayList<>(sortedMap.entrySet());

		for( int i = list.size() -1; i >= list.size() - 21 ; i --){
		    Entry<String, Float> me = list.get(i);
		    context.write(new Text(me.getKey().toString()), new Text(me.getValue().toString()));
		}
	}
	public LinkedHashMap sortHashMapByValuesD(Map<String,  Float> userAgeMap2) {
		   List mapKeys = new ArrayList(userAgeMap2.keySet());
		   List mapValues = new ArrayList(userAgeMap2.values());
		   Collections.sort(mapValues);
		   Collections.sort(mapKeys);

		   LinkedHashMap sortedMap = new LinkedHashMap();
		   Iterator valueIt = mapValues.iterator();
		   while (valueIt.hasNext()) {
		       Object val = valueIt.next();
		       Iterator keyIt = mapKeys.iterator();
		       while (keyIt.hasNext()) {
		           Object key = keyIt.next();
		           String comp1 = userAgeMap2.get(key).toString();
		           String comp2 = val.toString();

		           if (comp1.equals(comp2)){
		               userAgeMap2.remove(key);
		               mapKeys.remove(key);
		               sortedMap.put((String)key, (Float)val);
		               break;
		           }
		       }
		   }
		   return sortedMap;
	}
}