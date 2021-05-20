import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class pp_mapper implements Mapper<LongWritable,Text,Text,Text>{
	public void map(LongWritable key, Text value,OutputCollector<Text, Text> output, Reporter rep)
			throws IOException{
		String val = value.toString();
		String nodes[] = val.split("\t")[0].split(",");
		output.collect(new Text(nodes[0]), new Text(nodes[1]));
		output.collect(new Text(nodes[1]), new Text(nodes[0]));
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