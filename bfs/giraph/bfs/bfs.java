package org.apache.giraph.examples;

import org.apache.giraph.aggregators.BooleanOrAggregator;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.giraph.worker.WorkerContext;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.giraph.Algorithm;

@Algorithm(name = "bfs", description = "bfs")
public class bfs extends
		Vertex<IntWritable, IntWritable, NullWritable, ByteWritable> {

	public static int DEST_VERTEX;
	public static String DEST_STRING;
	
	public static String halt = "halt";

	@Override
	public void compute(Iterable<ByteWritable> messages) {

		if (bfsWorkerContext.getHalt()) {
			this.voteToHalt();
		}

		if (this.getSuperstep() == 0) {
			if (DEST_STRING == null) {
				DEST_STRING = getConf().get("dest", "-1");
				DEST_VERTEX = Integer.parseInt(DEST_STRING);
			}
			if (this.getValue().get() != Integer.MAX_VALUE) {

				this.sendMessageToAllEdges(new ByteWritable((byte) 1));
			}

		} else {
			if (this.getValue().get() == Integer.MAX_VALUE) {
				this.setValue(new IntWritable((int) this.getSuperstep()));
				this.sendMessageToAllEdges(new ByteWritable((byte) 1));
			}
		}

		if (this.getId().get() == DEST_VERTEX
				&& this.getValue().get() != Integer.MAX_VALUE) {
			aggregate(halt, new BooleanWritable(true));
		}

		this.voteToHalt();

	}

	public static class bfsWorkerContext extends WorkerContext {

		private static boolean isHalt;

		public static boolean getHalt() {
			return isHalt;
		}

		@Override
		public void preSuperstep() {
			isHalt = (this.<BooleanWritable> getAggregatedValue(halt)).get();
		}

		@Override
		public void postSuperstep() {

		}

		@Override
		public void postApplication() {

		}

		@Override
		public void preApplication() throws InstantiationException,
				IllegalAccessException {
			// TODO Auto-generated method stub
		}
	}

	/**
	 * Master compute associated with {@link SimplePageRankComputation}. It
	 * registers required aggregators.
	 */
	public static class bfsMasterCompute extends DefaultMasterCompute {
		@Override
		public void initialize() throws InstantiationException,
				IllegalAccessException {
			registerAggregator(halt, BooleanOrAggregator.class);
		}
	}
}
