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

public class pp_reducer implements Reducer<Text,Text,Text,Text>{

	public void reduce(Text key, Iterator<Text> value, OutputCollector<Text, Text> output, Reporter rep)
			throws IOException {
		ArrayList <String> adj_list = new ArrayList<String> ();
		while(value.hasNext()) {
			String s = value.next().toString();
			adj_list.add(s);
		}
		String s = String.join(",", adj_list);
		String struct = "1:"+key.toString()+":"+s;
		output.collect(key, new Text(struct));
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