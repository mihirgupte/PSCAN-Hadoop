import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class cc_mapper implements Mapper<LongWritable,Text,Text,Text>{
	public void map(LongWritable key, Text value,OutputCollector<Text, Text> output, Reporter rep)
			throws IOException{
		String val = value.toString();
		String info[] = val.split("\t");
		String vertex_id = info[0];
		String struct = info[1];
		String struct_info[] = info[1].split(":");
		
		for(String s: struct_info[2].split(",")) {
			output.collect(new Text(s), new Text(struct_info[1]));
		}
		output.collect(new Text(vertex_id), new Text(struct));

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