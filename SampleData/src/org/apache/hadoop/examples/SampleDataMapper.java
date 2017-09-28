package org.apache.hadoop.examples;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import java.util.Random;
import java.util.ArrayList;

public class SampleDataMapper extends Mapper<Object,Text,Text,NullWritable> {
	private int row = 0;
	private int k = 0;
	private ArrayList<Text> result = new ArrayList();
	@Override
	protected void setup(Context context)
	throws IOException,InterruptedException
	{
		k = context.getConfiguration().getInt("100", 100);
	}
	Random r = new Random();
	@Override
	protected void map(Object key,Text value,Context context)
	throws IOException,InterruptedException
	{
		row++;
		if(row <=k )
			result.add(new Text(value));
		else
		{
			int p = r.nextInt(row);
			if(p<k)
				result.set(p, new Text(value));
			
		}
	}
	@Override
	protected void cleanup(Context context) 
	throws IOException,InterruptedException
	{
		for(int i=0;i<result.size();i++)
			context.write(result.get(i), NullWritable.get());
	}
}
