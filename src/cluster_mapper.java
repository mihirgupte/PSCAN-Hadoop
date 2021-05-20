import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class cluster_mapper implements Mapper<LongWritable,Text,Text,Text>{
	public void map(LongWritable key, Text value,OutputCollector<Text, Text> output, Reporter rep)
			throws IOException{
		String val[] = value.toString().split("\t");
		String node = val[0];
		String struct[] = val[1].split(":");
		String leader = struct[1];
		output.collect(new Text(leader), new Text(node));
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