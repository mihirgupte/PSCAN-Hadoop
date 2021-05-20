import java.io.IOException;
import java.util.Arrays;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapred.TextInputFormat;    
import org.apache.hadoop.mapred.TextOutputFormat;

public class driver extends Configured implements Tool{
	
	public String givePath(String paths){
        String s = paths;
        String path[] = s.split("/");
        String p[] = Arrays.copyOfRange(path, 0, path.length-1);
        return String.join("/",p);
     }
	
	public String checkPath(String paths) {
		String s = paths;
		if(s.endsWith("/")){
            String path[] = s.split("/");
            String p[] = Arrays.copyOfRange(path, 0, path.length);
            return String.join("/",p);
        }
		return paths;
	}

    @Override
    public int run(String[] args) throws Exception {
        if(args.length<3) {
            System.out.println("Invalid Input");
            return -1;
        }
        // Map Reduce to return edges with structural similarity
        JobConf job = new JobConf(driver.class);
        args[1] = checkPath(args[1]);
        args[1] = args[1]+"/structure-similarity";
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setJobName("structural similarity");
        job.setMapperClass((Class<? extends org.apache.hadoop.mapred.Mapper>) ss_mapper.class);
        job.setReducerClass((Class<? extends org.apache.hadoop.mapred.Reducer>) ss_reducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        job.setInputFormat(TextInputFormat.class);
        job.setOutputFormat(TextOutputFormat.class);
        JobClient.runJob(job);
        
        // Map Reduce to convert the returned edges for next MR
        job = new JobConf(driver.class);
        args[0] = args[1]+"/part-00000";
        args[1] = givePath(args[1])+"/converted_graph";
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setJobName("processing edges with structure format");
        job.setMapperClass((Class<? extends org.apache.hadoop.mapred.Mapper>) pp_mapper.class);
        job.setReducerClass((Class<? extends org.apache.hadoop.mapred.Reducer>) pp_reducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        job.setInputFormat(TextInputFormat.class);
        job.setOutputFormat(TextOutputFormat.class);
        JobClient.runJob(job);
        
        //Map Reduce to form clusters
        args[0] = args[1]+"/part-00000";
        args[1] = givePath(args[1])+"/clusters/"+String.valueOf(0);
    	for(int i=0; i<Integer.parseInt(args[2]); i++) {
            job = new JobConf(driver.class);
            FileInputFormat.setInputPaths(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
            job.setJobName("making clusters");
            job.setMapperClass((Class<? extends org.apache.hadoop.mapred.Mapper>) cc_mapper.class);
            job.setReducerClass((Class<? extends org.apache.hadoop.mapred.Reducer>) cc_reducer.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(DoubleWritable.class);
            job.setInputFormat(TextInputFormat.class);
            job.setOutputFormat(TextOutputFormat.class);
            JobClient.runJob(job);
            args[0] = args[1]+"/part-00000";
            args[1] = args[1]+String.valueOf(i);
    	}
    	
    	//Map Reduce to output clusters
    	args[1] = givePath(givePath(args[1]))+"/final";
    	job = new JobConf(driver.class);
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setJobName("prettifying");
        job.setMapperClass((Class<? extends org.apache.hadoop.mapred.Mapper>) cluster_mapper.class);
        job.setReducerClass((Class<? extends org.apache.hadoop.mapred.Reducer>) cluster_reducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        job.setInputFormat(TextInputFormat.class);
        job.setOutputFormat(TextOutputFormat.class);
        JobClient.runJob(job);
        
        return 0;
    }
    
    public static void main(String args[]) throws Exception
    {
        int exitCode = ToolRunner.run(new driver(), args);
        System.out.println(exitCode);
    }

}