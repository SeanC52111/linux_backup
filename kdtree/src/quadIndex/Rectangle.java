package quadIndex;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Writable;
public class Rectangle extends SpatialObj implements Writable{
	public double x;
	public double y;
	public double width;
	public double height;
	public double getWidth() {
		return width;
	}
	public Rectangle(double x2,double y2,double d,double e)
	{
		x = x2;
		y = y2;
		width = d;
		height = e;
	}
	public boolean isEmpty()
	{
		if(width ==0 || height ==0)
			return true;
		return false;
	}
	
	public boolean contains(Rectangle rect) {
		if(x <= rect.x && y <= rect.y)
		{
			if(x+width>=rect.x+rect.width && y+height>=rect.y+rect.height) {
				return true;
			}
		}
		return false;
	}
	
	private boolean isOverlap(double x1,double x2,double _x1,double _x2)
	{
		if(x2<_x1 || x1 > _x2)
			return false;
		return true;
	}
	
	@Override
	public boolean intersects(Rectangle rect) {
		if(isOverlap(x,x+width,rect.x,rect.x+rect.width)&&isOverlap(y,y+height,rect.y,rect.y+rect.height)) {
			return true;
		}
		return false;
	}
	@Override
	public void readFields(DataInput in)throws IOException{
		x = in.readDouble();
		y = in.readDouble();
		width = in.readDouble();
		height = in.readDouble();
	}
	@Override
	public void write(DataOutput out)throws IOException{
		out.writeDouble(x);
		out.writeDouble(y);
		out.writeDouble(width);
		out.writeDouble(height);
	}
	@Override
	public int getType() {
		return RECT;
	}
	public Iterator<Point> iterator(){
		return new RectIterator();
	}
	public class RectIterator implements Iterator<Point>{
		int count = 0;
		public boolean hasNext() {
			if(count<4) {
				return true;
			}
			return false;
		}
		public Point next() {
			count++;
			return new Point(x+count/2*width,y+count%2*height);
		}
		public void remove() {
			
		}
	}
	@Override
	public void DebugPrint() {
		System.out.println("[RECT]"+x+" "+y+" "+width+" "+height);
	}
	@Override
	public int size() {
		return 4*8;
	}
	@Override
	public Rectangle getMBR() {
		return this;
	}
	@Override
	public String toString() {
		return "[RECT]"+x+" "+y+" "+width+" "+height;
	}
}
