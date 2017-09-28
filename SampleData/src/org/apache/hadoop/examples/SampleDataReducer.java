package org.apache.hadoop.examples;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Random;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.io.NullWritable;
public class SampleDataReducer extends Reducer<Text,NullWritable,Text,NullWritable>
{
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
	protected void reduce(Text key, Iterable<NullWritable>values, Context context)
	throws IOException,InterruptedException{
		row++;
		if(row <=k )
			result.add(new Text(key));
		else
		{
			int p = r.nextInt(row);
			if(p<k)
				result.set(p, new Text(key));
			
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