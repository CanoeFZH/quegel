package org.apache.giraph.examples;

import org.apache.giraph.aggregators.IntMinAggregator;
import org.apache.giraph.aggregators.IntSumAggregator;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.giraph.worker.WorkerContext;

import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.giraph.Algorithm;

@Algorithm(name = "bibfsDG", description = "bibfsDG")
public class bibfsDG extends
		Vertex<IntWritable, bibfsWritable, IntWritable, ByteWritable> {

	public static int SOURCE_VERTEX;
	public static String SOURCE_STRING;
	public static int DEST_VERTEX;
	public static String DEST_STRING;

	public static String min_dist = "min_dist";
	public static String forward_cover = "forward_cover";
	public static String backward_cover = "backward_cover";

	@Override
	public void compute(Iterable<ByteWritable> messages) {


		if (bibfsDGWorkerContext.getDist() != Integer.MAX_VALUE) {
			this.getValue().setFdist(bibfsDGWorkerContext.getDist());
			this.voteToHalt();
			return;
		}

		if (this.getSuperstep() == 0) {
			if (SOURCE_STRING == null) {
				SOURCE_STRING = getConf().get("source", "-1");
				SOURCE_VERTEX = Integer.parseInt(SOURCE_STRING);
			}
			if (DEST_STRING == null) {
				DEST_STRING = getConf().get("dest", "-1");
				DEST_VERTEX = Integer.parseInt(DEST_STRING);
			}

			if (this.getId().get() == SOURCE_VERTEX) {
				for(Edge<IntWritable,IntWritable> edge: this.getEdges())
				{
					if(edge.getValue().get() == 1) // out_edge
					{
						this.sendMessage(edge.getTargetVertexId(), new ByteWritable((byte) 1));
					}
				}
				aggregate(forward_cover, new IntWritable(1));
			} else if (this.getId().get() == DEST_VERTEX) {
				for(Edge<IntWritable,IntWritable> edge: this.getEdges())
				{
					if(edge.getValue().get() == 0) // in_edge
					{
						this.sendMessage(edge.getTargetVertexId(), new ByteWritable((byte) 2));
					}
				}
				aggregate(backward_cover, new IntWritable(1));
			}
		} else {
			int msg = 0;
			for (ByteWritable message : messages) {
				msg |= message.get();
			}

			int old_value = this.getValue().getFlag();

			if ((old_value & 1) == 0 && ((msg & 1) == 1)) // forward;
			{
				this.getValue().setFdist((int) this.getSuperstep());
				for(Edge<IntWritable,IntWritable> edge: this.getEdges())
				{
					if(edge.getValue().get() == 1) // out_edge
					{
						this.sendMessage(edge.getTargetVertexId(), new ByteWritable((byte) 1));
					}
				}
				aggregate(forward_cover, new IntWritable(1));
			}

			if ((old_value & 2) == 0 && ((msg & 2) == 2)) // backward;
			{
				this.getValue().setBdist((int) this.getSuperstep());
				for(Edge<IntWritable,IntWritable> edge: this.getEdges())
				{
					if(edge.getValue().get() == 0) // in_edge
					{
						this.sendMessage(edge.getTargetVertexId(), new ByteWritable((byte) 2));
					}
				}
				aggregate(backward_cover, new IntWritable(1));
			}
			this.getValue().setFlag(msg | old_value);

			if (this.getValue().getFlag() == 3) {
				if(this.getValue().getFdist() != Integer.MAX_VALUE && this.getValue().getBdist() !=  Integer.MAX_VALUE)
				{
					aggregate(min_dist, new IntWritable( this.getValue().getFdist() + this.getValue().getBdist()));
				}
			}
		}
		/*
		System.out.println("step: " + this.getSuperstep() + " id: " + this.getId().get() + " flag: "
				+ this.getValue().getFlag() + " fdist: "
				+ this.getValue().getFdist() + " bdist: "
				+ this.getValue().getBdist());
		*/
		if(this.getId().get() != DEST_VERTEX)
			this.voteToHalt();

	}

	public static class bibfsDGWorkerContext extends WorkerContext {

		private static int dist = Integer.MAX_VALUE;

		public static int getDist() {
			return dist;
		}


		@Override
		public void preSuperstep() {

			if (this.getSuperstep() > 0) {

				if (dist == Integer.MAX_VALUE) {
					dist = (this.<IntWritable> getAggregatedValue(min_dist))
							.get();
					
				}
				// System.out.println("step: " + this.getSuperstep() + " dist: " + dist);
				if(dist != Integer.MAX_VALUE)
					return;
				
				int fnum = (this
						.<IntWritable> getAggregatedValue(forward_cover)).get();
				int bnum = (this
						.<IntWritable> getAggregatedValue(backward_cover))
						.get();
				//System.out.println("step: " + this.getSuperstep() + " fnum: " + fnum + " bnum: " + bnum );
				if (fnum == 0 || bnum == 0) {
					dist = -1; // terminate;
				}
			}
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

	public static class bibfsDGMasterCompute extends DefaultMasterCompute {
		@Override
		public void initialize() throws InstantiationException,
				IllegalAccessException {
			registerAggregator(min_dist, IntMinAggregator.class);
			registerAggregator(forward_cover, IntSumAggregator.class);
			registerAggregator(backward_cover, IntSumAggregator.class);
		}
	}
}
