
package org.apache.giraph.examples;

import org.apache.giraph.combiner.Combiner;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.IntWritable;

public class bfsCombiner extends Combiner<IntWritable, ByteWritable> {
    @Override
    public void combine(IntWritable vertexIndex, ByteWritable originalMessage,
    		ByteWritable messageToCombine) {
	originalMessage.set(originalMessage.get());
    }

    @Override
    public ByteWritable createInitialMessage() {
	return new ByteWritable((byte)1);
    }
}