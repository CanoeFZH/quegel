package org.apache.giraph.examples;

import org.apache.giraph.combiner.Combiner;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.IntWritable;

public class bibfsCombiner extends Combiner<IntWritable, ByteWritable> {
	@Override
	public void combine(IntWritable vertexIndex, ByteWritable originalMessage,
			ByteWritable messageToCombine) {
		originalMessage.set((byte)(originalMessage.get() | messageToCombine.get()));
	}

	@Override
	public ByteWritable createInitialMessage() {
		return new ByteWritable((byte)0);
	}
}