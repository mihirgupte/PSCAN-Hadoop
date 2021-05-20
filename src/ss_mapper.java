import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class ss_mapper implements Mapper<LongWritable,Text,Text,Text>{
	public void map(LongWritable key, Text value,OutputCollector<Text, Text> output, Reporter rep)
			throws IOException{
		// TODO Auto-generated method stub
		for(String val: value.toString().split("\n")) {
			String res[] = val.split(" ");
			int vertex = Integer.parseInt(res[0]);
			String list[] = res[1].split(",");
			int j = 0;
			String s="";
			for(int i=0; i<list.length; i++) {
				j = Integer.parseInt(list[i]);
				if(j<vertex) {
					s = list[i]+","+res[0];
				}
				else {
					s = res[0]+","+list[i];
				}
				output.collect(new Text(s),new Text(value.toString().split(" ")[1]));
			}
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