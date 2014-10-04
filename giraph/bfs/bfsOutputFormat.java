package org.apache.giraph.examples;

import java.io.IOException;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.giraph.io.formats.TextVertexOutputFormat;

public class bfsOutputFormat extends
		TextVertexOutputFormat<IntWritable, IntWritable, NullWritable> {
	public static final String LINE_TOKENIZE_VALUE_DEFAULT = "\t";
	public static int DEST_VERTEX;
	public static String DEST_STRING;

	@Override
	public TextVertexWriter createVertexWriter(TaskAttemptContext context) {
		return new bfsVertexWriter();
	}

	/**
	 * Vertex writer used with {@link IdWithValueTextOutputFormat}.
	 */
	protected class bfsVertexWriter extends TextVertexWriterToEachLine {
		/** Saved delimiter */
		private String delimiter = "\t";

		@Override
		protected Text convertVertexToLine(
				Vertex<IntWritable, IntWritable, NullWritable, ?> vertex)
				throws IOException {
			

			if (DEST_STRING == null) {
				DEST_STRING = getConf().get("dest", "-1");
				DEST_VERTEX = Integer.parseInt(DEST_STRING);
			}
			StringBuilder str = new StringBuilder();
			if (vertex.getId().get() == DEST_VERTEX) {
				str.append(vertex.getId().toString());
				str.append(delimiter);
				str.append(vertex.getValue().toString());
			}

			return new Text(str.toString());
		}
	}
}