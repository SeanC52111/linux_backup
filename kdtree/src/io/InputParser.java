package io;

import java.nio.ByteBuffer;
import quadIndex.Point;
import quadIndex.Polygon;
import quadIndex.Rectangle;
import quadIndex.SpatialObj;
import java.util.*;


public class InputParser {
	public static SpatialObj getObjFromLine(String line) {
		if(line.contains("EMPTY")) {
			return null;
		}
		int pos1 = line.indexOf('(');
		int pos2 = line.indexOf(')');
		String data = line.substring(pos1+1, pos2);
		String delims = "[,| ]+";
		String[] strings = data.split(delims);
		if((strings.length & 1) == 1)
			return null;
		List<Double> nums = new ArrayList<Double>();
		for(int i = 0;i<strings.length;i++)
			nums.add(Double.parseDouble(strings[i]));
		int npoints = strings.length/2;
		
		double [] xs = new double[npoints];
		double [] ys = new double[npoints];
		for(int i = 0;i<npoints;i++)
		{
			xs[i] = nums.get(2*i);
			ys[i] = nums.get(2*i+1);
		}
		return new Polygon(xs,ys,npoints);
	}
	
	public static SpatialObj getObjFromBytes(byte[] bytes)
	{
		int len = bytes.length;
		ByteBuffer wrapped = ByteBuffer.wrap(bytes);
		int num = len/4;
		if(num<0)
			return null;
		else if(num == 2)
		{
			double x = wrapped.getDouble();
			double y = wrapped.getDouble();
			return new Point(x,y);
		}
		else if(num==4) {
			double x= wrapped.getDouble();
			double y = wrapped.getDouble();
			double w = wrapped.getDouble();
			double h = wrapped.getDouble();
			return new Rectangle(x,y,w,h);
		}
		else {
			int npoints = wrapped.getInt();
			double x[] = new double[npoints];
			double y[] = new double[npoints];
			for(int i=0;i<npoints;i++) {
				x[i]=wrapped.getDouble();
			}
			for(int i=0;i<npoints;i++) {
				y[i]=wrapped.getDouble();
			}
			return new Polygon(x,y,npoints);
		}
	}
}
