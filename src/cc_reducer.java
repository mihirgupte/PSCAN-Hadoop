import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.stream.IntStream;
import java.lang.Math;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class cc_reducer implements Reducer<Text,Text,Text,Text>{

	public void reduce(Text key, Iterator<Text> value, OutputCollector<Text, Text> output, Reporter rep)
			throws IOException {
		String struct="";
		int min = 999999999;
		while(value.hasNext()) {
			String val = value.next().toString();
			if(val.contains(":")) {
				struct = val;
				continue;
			}
			int min1 = Integer.parseInt(val);
			if(min>min1) {
				min = min1;
			}
		}
		String info[] = struct.split(":");
		int label = Integer.parseInt(info[1]);

		if(label>min) {
			info[1] = String.valueOf(min);
			String s = String.join(":",info);
			output.collect(key, new Text(s));
		}
		else {
			info[0] = "0";
			String s = String.join(":",info);
			output.collect(key, new Text(s));
		}

	}

	@Override
	public void configure(JobConf arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		
	}


}