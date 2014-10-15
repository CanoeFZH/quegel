package org.apache.giraph.examples;


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;


public class bibfsWritable implements Writable {
	private int flag;
	private int fdist;
	private int bdist;
	
	public bibfsWritable(int _flag,int _fdist, int _bdist) {
		this.flag = _flag;
		this.fdist = _fdist;
		this.bdist = _bdist;
	}
	public bibfsWritable(){
	
	}
	public int getFlag()
	{
		return flag;
	}
	public int getFdist()
	{
		return fdist;
	}
	public int getBdist()
	{
		return bdist;
	}
	
	public void setFlag(int _flag)
	{
		this.flag = _flag;
	}
	
	public void setFdist(int _fdist)
	{
		this.fdist = _fdist;
	}
	
	public void setBdist(int _bdist)
	{
		this.bdist = _bdist;
	}
	public void write(DataOutput out) throws IOException {
		out.writeInt(flag);
		out.writeInt(fdist);
		out.writeInt(bdist);
	}

	public void readFields(DataInput in) throws IOException {
		flag = in.readInt();
		fdist = in.readInt();
		bdist = in.readInt();
	}

	public static bibfsWritable read(DataInput in) throws IOException {
		bibfsWritable w = new bibfsWritable();
		w.readFields(in);
		return w;
	}
}