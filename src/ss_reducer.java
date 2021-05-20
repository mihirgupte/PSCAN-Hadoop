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
import org.apache.hadoop.shaded.org.apache.commons.math3.analysis.function.Sqrt;

public class ss_reducer implements Reducer<Text,Text,Text,DoubleWritable>{

	public void reduce(Text key, Iterator<Text> value, OutputCollector<Text, DoubleWritable> output, Reporter rep)
			throws IOException {
		double threshold = 0.5;
		double ss = 1,prod=1;
		ArrayList<Integer> adj_list = new ArrayList<Integer> ();
		while(value.hasNext()) {
			Text t = value.next();
			String list[] = t.toString().split(",");
			
			prod=prod*(list.length+1);
			for(int i=0; i<list.length; i++) {
				int val = Integer.parseInt(list[i]);
				int c = 0;
				for(int j=0; j<adj_list.size(); j++) {
					if(val==adj_list.get(j)) {
						c=1;
					}
				}
				if(c==1) {
					adj_list.add(val);
				}
			}
			
		}
		ss = (adj_list.size()+2)/Math.sqrt(prod);
		if(ss>threshold) {
		output.collect(key, new DoubleWritable(ss));
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