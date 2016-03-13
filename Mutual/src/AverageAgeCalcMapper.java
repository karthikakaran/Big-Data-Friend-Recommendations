import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AverageAgeCalcMapper extends Mapper  <LongWritable, Text, Text, Text>
{
	private String userId,userFriendList;
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		String line = value.toString();
		if(line != null){
			String splitarray[] = line.split("\\t");
			userId = splitarray[0].trim();
			if(splitarray.length > 1){
				userFriendList = splitarray[1].trim();
				context.write(new Text(userId), new Text(userFriendList));	
			}
		}
	}
}