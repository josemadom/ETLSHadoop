package es.unex.etlhadoop.main;

//Proyecto
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import es.unex.etlhadoop.mapper.ETLHadoopMapper;
import es.unex.etlhadoop.reducer.ETLHadoopReducer;

public class ETLHadoop 
{

	public static void main(String[] args) throws Exception 
	{
		if (args.length != 2) 
		{
			System.err.println("Usage: Proyecto <input path> <output path>");
			System.exit(-1);
		}

		Job job = new Job();
		job.setJarByClass(ETLHadoop.class);
		job.setJobName("ETLHadoop TFG");

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapperClass(ETLHadoopMapper.class);
		job.setReducerClass(ETLHadoopReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}