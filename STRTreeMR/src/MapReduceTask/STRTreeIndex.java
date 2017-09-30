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

import quadIndex.Rect;
import debug.Debug;
import STRTree.STRTreeWritable;


public class STRTreeIndex {
	public static void run(String []args) {
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
		JobConf conf = new JobConf(STRTreeIndex.class);
		conf.setJobName("index");
		conf.setMapperClass(STRTreeIndexMapper.class);
		conf.setReducerClass(STRTreeIndexReducer.class);
		
		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(Rect.class);
		
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(STRTreeWritable.class);
		conf.setOutputFormat(SequenceFileOutputFormat.class);
		
		FileInputFormat.setInputPaths(conf, new Path(in_path));
		FileOutputFormat.setOutputPath(conf, new Path(out_path));
		
		try {
			JobClient.runJob(conf);
		}
		catch(IOException e){
			e.printStackTrace();
		}
	}
}


//input (key,value) = lineno, line
//output (key value) = count to number of calls of map, Rect
class STRTreeIndexMapper extends MapReduceBase
	implements Mapper<LongWritable,Text,Text,Rect>{
	static int inst_count = 0;
	static enum IndexCounters {SIZE_E,COUNT_E};
	
	synchronized Text getTreeId(Text content,Reporter rpt) {
		String path = ((FileSplit)rpt.getInputSplit()).getPath().toString();
		return new Text(path);
	}
	
	@Override
	public void map(LongWritable no,Text content,OutputCollector<Text,Rect> oc,Reporter rpt) throws IOException{
		String line = content.toString();
		Rect rect = InputParser.getObjFromLine(line);
		Text tid = getTreeId(content,rpt);
		Debug.println("Tree id= "+tid.toString());
		oc.collect(tid, rect);
	}
}

//input (key,value) = count in each mapper, Rect
//output (key,value) = _, STRTree

class STRTreeIndexReducer extends MapReduceBase
	implements Reducer<Text,Rect,Text,STRTreeWritable>{
	static int count = 0;
	static int nodec = 0;
	
	@Override
	public void configure(JobConf job) {
		nodec = job.getInt("nodec", 3);
	}
	
	@Override
	public void reduce(Text key,Iterator<Rect> value,OutputCollector<Text,STRTreeWritable> oc,Reporter rpt)throws IOException{
		count++;
		ArrayList<Rect> rlist = new ArrayList<Rect>();
		while(value.hasNext()) {
			Rect r = value.next();
			//Debug.println(r.toString());
			rlist.add(r);
		}
		STRTreeWritable strtree = new STRTreeWritable(rlist,3);
		oc.collect(key, strtree);
	}
}










