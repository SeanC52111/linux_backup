package MapReduceTask;
import io.InputParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.FileSplit;

import quadIndex.SpatialObj;
import debug.Debug;
import Shape.Rect;
import kdtree.KDTreeWritable;

public class KDTreeIndex {
	public static void run(String [] args)
	{
		String in_path = null;
		String out_path = null;
		if(args.length<2) {
			in_path = "input";
			out_path = "tree";
		}
		else {
			in_path = args[0];
			out_path = args[1];
		}
		JobConf conf = new JobConf(KDTreeIndex.class);
		conf.setJobName("index");
		conf.setMapperClass(KDTreeIndexMapper.class);
		conf.setReducerClass(KDTreeIndexReducer.class);
		
		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(Rect.class);
		
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(KDTreeWritable.class);
		conf.setOutputFormat(SequenceFileOutputFormat.class);
		
		FileInputFormat.setInputPaths(conf, new Path(in_path));
		FileOutputFormat.setOutputPath(conf, new Path(out_path));
		
		try {
			JobClient.runJob(conf);
		}catch(IOException e) {
			e.printStackTrace();
		}
	}
}


//input (key,value) = lineno,line
//output (key,value) = count to number of calls of map, Rect

class KDTreeIndexMapper extends MapReduceBase
	implements Mapper<LongWritable,Text,Text,Rect>{
	static int inst_count = 0;
	static enum IndexCounters {SIZE_E,COUNT_E};
	final static int sizeof_boolean = 1;
	final static int sizeof_int = Integer.SIZE;
	final static int max_block_size = 1048576;
	
	synchronized Text getTreeId(Text content,Reporter rpt) {
		//Counters.Counter size_cc = rpt.getCounter(IndexCounters.SIZE_E);
		//Counters.Counter count_cc = rpt.getCounter(IndexCounters.COUNT_E);
		//int dsize = rect.size() +sizeof_boolean*2 +sizeof_int;
		//int dsize = content.getBytes().length+1;
		//Debug.println("size:"+size_cc.getValue());
		/*
		if(dsize+size_cc.getValue()>max_block_size) {
			size_cc.setValue(dsize);
			count_cc.increment(1);
			return new LongWritable(count_cc.getValue());
		}
		else {
			size_cc.increment(dsize);
			return new LongWritable(count_cc.getValue());
		}
		*/
		String path = ((FileSplit)rpt.getInputSplit()).getPath().toString();
		return new Text(path);
	}
	
	@Override
	public void map(LongWritable no,Text content,OutputCollector<Text,Rect> oc,Reporter rpt) throws IOException{
		String line = content.toString();
		SpatialObj obj = InputParser.getObjFromLine(line);
		Rect rect = new Rect(obj);
		Text tid = getTreeId(content,rpt);
		Debug.println("Tree id = "+tid.toString());
		oc.collect(tid, rect);
	}

}

//input (key,value) = count in each mapper, Rect
//output (key,value) = _,KDTree

class KDTreeIndexReducer extends MapReduceBase
	implements Reducer<Text,Rect,Text,KDTreeWritable>{
	static int count = 0;
	@Override
	public void reduce(Text key,Iterator<Rect> value,OutputCollector<Text,KDTreeWritable> oc,Reporter rpt)throws IOException{
		count++;
		ArrayList<Rect> rlist = new ArrayList<Rect>();
		while(value.hasNext()) {
			Rect r = value.next();
			rlist.add(new Rect(r));
		}
		Rect[] rect_list = new Rect[rlist.size()];
		rect_list = rlist.toArray(rect_list);
		KDTreeWritable kdtree = new KDTreeWritable(rect_list);
		
		oc.collect(key, kdtree);
	}
}
















