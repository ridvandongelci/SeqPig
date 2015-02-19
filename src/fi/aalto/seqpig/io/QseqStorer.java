// Copyright (c) 2012 Aalto University
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to
// deal in the Software without restriction, including without limitation the
// rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
// sell copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
// FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
// IN THE SOFTWARE.

package fi.aalto.seqpig.io;

import org.apache.pig.StoreFunc;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.spark.SparkUtil;
import org.apache.pig.PigException;
import org.apache.pig.ResourceSchema;
import org.apache.pig.impl.util.UDFContext;
import org.apache.pig.spark.StoreSparkFunc;
import org.apache.spark.rdd.PairRDDFunctions;
import org.apache.spark.rdd.RDD;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.Properties;
import java.util.regex.Pattern;
import java.io.ByteArrayOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ObjectOutputStream;
import java.io.ObjectInputStream;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.net.URI;

import fi.tkk.ics.hadoop.bam.FastqOutputFormat;
import fi.tkk.ics.hadoop.bam.QseqOutputFormat;
import fi.tkk.ics.hadoop.bam.QseqOutputFormat.QseqRecordWriter;
import fi.tkk.ics.hadoop.bam.SequencedFragment;

import org.apache.commons.codec.binary.Base64;

import scala.Tuple2;
import scala.collection.Iterator;
import scala.runtime.AbstractFunction1;

public class QseqStorer extends StoreFunc implements StoreSparkFunc {
	protected RecordWriter writer = null;
	protected HashMap<String, Integer> allQseqFieldNames = null;

	// tuple format:
	//
	// instrument:string
	// run_number:int
	// flow_cell_id: string
	// lane: int
	// tile: int
	// xpos: int
	// ypos: int
	// read: int
	// qc_passed (a.k.a. filter): boolean
	// control_number: int
	// index_sequence: string
	// sequence: string
	// quality: string (note: we assume that encoding chosen on command line!!!)

	public QseqStorer() {
	}

	@Override
	public void putNext(Tuple f) throws IOException {

		initAllQseqFieldNames();

		SequencedFragment fastqrec = tupleToSequencedFragment(f,
				allQseqFieldNames);

		try {
			writer.write(null, fastqrec);
		} catch (InterruptedException e) {
			throw new IOException(e);
		}
	}

	/**
	 * @throws IOException
	 */
	private void initAllQseqFieldNames() throws IOException {
		if (allQseqFieldNames == null) {
			try {
				// BASE64Decoder decode = new BASE64Decoder();
				Base64 codec = new Base64();
				Properties p = UDFContext.getUDFContext().getUDFProperties(
						this.getClass());
				String datastr = p.getProperty("allQseqFieldNames");
				// byte[] buffer = decode.decodeBuffer(datastr);
				byte[] buffer = codec.decodeBase64(datastr);
				ByteArrayInputStream bstream = new ByteArrayInputStream(buffer);
				ObjectInputStream ostream = new ObjectInputStream(bstream);

				allQseqFieldNames = (HashMap<String, Integer>) ostream
						.readObject();
			} catch (ClassNotFoundException e) {
				throw new IOException(e);
			}
		}
	}

	/**
	 * @param tuple
	 * @return
	 * @throws ExecException
	 */
	private static SequencedFragment tupleToSequencedFragment(Tuple tuple,
			HashMap<String, Integer> allQseqFieldNames) throws ExecException {
		SequencedFragment fastqrec = new SequencedFragment();
		int index;

		index = getFieldIndex("instrument", allQseqFieldNames);
		if (index > -1
				&& DataType.findType(tuple.get(index)) == DataType.CHARARRAY) {
			fastqrec.setInstrument((String) tuple.get(index));
		}

		index = getFieldIndex("run_number", allQseqFieldNames);
		if (index > -1
				&& DataType.findType(tuple.get(index)) == DataType.INTEGER) {
			fastqrec.setRunNumber(((Integer) tuple.get(index)));
		}

		index = getFieldIndex("flow_cell_id", allQseqFieldNames);
		if (index > -1
				&& DataType.findType(tuple.get(index)) == DataType.CHARARRAY) {
			fastqrec.setFlowcellId((String) tuple.get(index));
		}

		index = getFieldIndex("lane", allQseqFieldNames);
		if (index > -1
				&& DataType.findType(tuple.get(index)) == DataType.INTEGER) {
			fastqrec.setLane(((Integer) tuple.get(index)));
		}

		index = getFieldIndex("tile", allQseqFieldNames);
		if (index > -1
				&& DataType.findType(tuple.get(index)) == DataType.INTEGER) {
			fastqrec.setTile(((Integer) tuple.get(index)));
		}

		index = getFieldIndex("xpos", allQseqFieldNames);
		if (index > -1
				&& DataType.findType(tuple.get(index)) == DataType.INTEGER) {
			fastqrec.setXpos(((Integer) tuple.get(index)));
		}

		index = getFieldIndex("ypos", allQseqFieldNames);
		if (index > -1
				&& DataType.findType(tuple.get(index)) == DataType.INTEGER) {
			fastqrec.setYpos(((Integer) tuple.get(index)));
		}

		index = getFieldIndex("read", allQseqFieldNames);
		if (index > -1
				&& DataType.findType(tuple.get(index)) == DataType.INTEGER) {
			fastqrec.setRead(((Integer) tuple.get(index)));
		}

		index = getFieldIndex("qc_passed", allQseqFieldNames);
		if (index > -1
				&& DataType.findType(tuple.get(index)) == DataType.BOOLEAN) {
			fastqrec.setFilterPassed(((Boolean) tuple.get(index)));
		}

		index = getFieldIndex("control_number", allQseqFieldNames);
		if (index > -1
				&& DataType.findType(tuple.get(index)) == DataType.INTEGER) {
			fastqrec.setControlNumber(((Integer) tuple.get(index)));
		}

		index = getFieldIndex("index_sequence", allQseqFieldNames);
		if (index > -1
				&& DataType.findType(tuple.get(index)) == DataType.CHARARRAY) {
			fastqrec.setIndexSequence((String) tuple.get(index));
		}

		index = getFieldIndex("sequence", allQseqFieldNames);
		if (index > -1
				&& DataType.findType(tuple.get(index)) == DataType.CHARARRAY) {
			fastqrec.setSequence(new Text((String) tuple.get(index)));
		}

		index = getFieldIndex("quality", allQseqFieldNames);
		if (index > -1
				&& DataType.findType(tuple.get(index)) == DataType.CHARARRAY) {
			fastqrec.setQuality(new Text((String) tuple.get(index)));
		}
		return fastqrec;
	}

	private static int getFieldIndex(String field,
			HashMap<String, Integer> fieldNames) {
		if (!fieldNames.containsKey(field)) {
			System.err.println("Warning: field missing: " + field);
			return -1;
		}

		return ((Integer) fieldNames.get(field)).intValue();
	}

	@Override
	public void checkSchema(ResourceSchema s) throws IOException {

		allQseqFieldNames = new HashMap<String, Integer>();
		String[] fieldNames = s.fieldNames();

		for (int i = 0; i < fieldNames.length; i++) {
			// System.out.println("field: "+fieldNames[i]);
			allQseqFieldNames.put(fieldNames[i], new Integer(i));
		}

		if (!( /*
				 * allQseqFieldNames.containsKey("instrument") &&
				 * allQseqFieldNames.containsKey("run_number") &&
				 * allQseqFieldNames.containsKey("flow_cell_id") &&
				 * allQseqFieldNames.containsKey("lane") &&
				 * allQseqFieldNames.containsKey("tile") &&
				 * allQseqFieldNames.containsKey("xpos") &&
				 * allQseqFieldNames.containsKey("ypos") &&
				 * allQseqFieldNames.containsKey("read") &&
				 * allQseqFieldNames.containsKey("filter") &&
				 * allQseqFieldNames.containsKey("control_number") &&
				 * allQseqFieldNames.containsKey("index_sequence")
				 */
		allQseqFieldNames.containsKey("sequence") && allQseqFieldNames
				.containsKey("quality")))
			throw new IOException(
					"Error: Incorrect Qseq tuple-field name or compulsory field missing");

		// BASE64Encoder encode = new BASE64Encoder();
		Base64 codec = new Base64();
		Properties p = UDFContext.getUDFContext().getUDFProperties(
				this.getClass());
		String datastr;
		// p.setProperty("someproperty", "value");

		ByteArrayOutputStream bstream = new ByteArrayOutputStream();
		ObjectOutputStream ostream = new ObjectOutputStream(bstream);
		ostream.writeObject(allQseqFieldNames);
		ostream.close();
		// datastr = encode.encode(bstream.toByteArray());
		datastr = codec.encodeBase64String(bstream.toByteArray());
		p.setProperty("allQseqFieldNames", datastr); // new
														// String(bstream.toByteArray(),
														// "UTF8"));
	}

	@Override
	public OutputFormat getOutputFormat() {
		return new QseqOutputFormat();
	}

	@Override
	public void prepareToWrite(RecordWriter writer) {
		this.writer = writer;
	}

	@Override
	public void setStoreLocation(String location, Job job) throws IOException {
		FileOutputFormat.setOutputPath(job, new Path(location));
	}

	@Override
	public void putRDD(RDD<Tuple> rdd, String path, JobConf conf)
			throws IOException {
		initAllQseqFieldNames();

		FromTupleFunction FROM_TUPLE_FUNCTION = new FromTupleFunction(
				allQseqFieldNames);
		RDD<Tuple2<Text, SequencedFragment>> rddPairs = rdd.mapPartitions(
				FROM_TUPLE_FUNCTION, true,
				SparkUtil.<Text, SequencedFragment> getTuple2Manifest());
		PairRDDFunctions<Text, SequencedFragment> pairRDDFunctions = new PairRDDFunctions<Text, SequencedFragment>(
				rddPairs, SparkUtil.getManifest(Text.class),
				SparkUtil.getManifest(SequencedFragment.class), null);
		pairRDDFunctions.saveAsNewAPIHadoopFile(path, Text.class,
				SequencedFragment.class, QseqOutputFormat.class, conf);
	}

	private static class FromTupleFunction extends
			AbstractFunction1<Iterator<Tuple>, Iterator<Tuple2<Text, SequencedFragment>>> implements
			Serializable {

		private static Text EMPTY_TEXT = new Text();
		private HashMap<String, Integer> allQseqFieldNames;

		public FromTupleFunction(HashMap<String, Integer> allFastqFieldNames) {
			this.allQseqFieldNames = allFastqFieldNames;
		}

		@Override
		public Iterator<Tuple2<Text, SequencedFragment>> apply(
				Iterator<Tuple> input) {
			
			return input.map(new AbstractFunction1<Tuple, Tuple2<Text, SequencedFragment>>() {
				public Tuple2<Text, SequencedFragment> apply(Tuple v1) {
					try {

						return new Tuple2<Text, SequencedFragment>(EMPTY_TEXT,
								tupleToSequencedFragment(v1, allQseqFieldNames));
					} catch (ExecException e) {
						e.printStackTrace();
						throw new RuntimeException(e);
					}
				}
			});
		}

		
	}
}
