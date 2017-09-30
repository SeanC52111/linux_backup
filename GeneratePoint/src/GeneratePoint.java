import java.util.Random;
import java.io.*;
public class GeneratePoint {
	public static void main(String[] args) throws IOException {
		FileWriter fw = new FileWriter("data points.txt",false);
		for(int i = 0;i < 15;i++) {
			Random r = new Random();
			int x = r.nextInt(20);
			int y = r.nextInt(20);
			fw.write(""+(double)x+" "+(double)x+" "+(double)y+" "+(double)y+" "+"\n");
		}
		fw.close();
	}
}
