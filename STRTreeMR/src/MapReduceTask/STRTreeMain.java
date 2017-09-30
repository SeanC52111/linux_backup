package MapReduceTask;
import java.util.Arrays;

public class STRTreeMain {
	public static void main(String []args) {
		if(args.length<1)
		{
			System.out.println("invalid syntax.");
			return ;
		}
		String sub_cmd = args[0];
		String[] arguments = Arrays.copyOfRange(args, 1, args.length);
		if(sub_cmd.equals("index")) {
			STRTreeIndex.run(arguments);
		}else if(sub_cmd.equals("range_query")) {
			STRTreeRangeQuery.run(arguments);
		}
		else {
			System.out.println("Umrecognized sub commands.");
		}
	}
	
}
