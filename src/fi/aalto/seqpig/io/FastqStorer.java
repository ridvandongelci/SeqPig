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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Properties;

import javax.management.RuntimeErrorException;

import net.sf.samtools.SAMFileHeader;
import net.sf.samtools.SAMRecord;
import net.sf.samtools.SAMTextHeaderCodec;
import net.sf.samtools.SAMFileReader.ValidationStringency;
import net.sf.samtools.util.StringLineReader;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.pig.ResourceSchema;
import org.apache.pig.StoreFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.spark.SparkUtil;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.util.UDFContext;
import org.apache.pig.spark.StoreSparkFunc;
import org.apache.spark.rdd.PairRDDFunctions;
import org.apache.spark.rdd.RDD;

import scala.Tuple2;
import scala.runtime.AbstractFunction1;
import fi.aalto.seqpig.io.BamStorer.KeyIgnoringBAMOutputFormatExtended;
import fi.tkk.ics.hadoop.bam.FastqOutputFormat;
import fi.tkk.ics.hadoop.bam.SAMRecordWritable;
import fi.tkk.ics.hadoop.bam.SequencedFragment;

public class FastqStorer extends StoreFunc implements StoreSparkFunc {
	protected RecordWriter writer = null;
	protected HashMap<String, Integer> allFastqFieldNames = null;

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

	public FastqStorer() {
	}

	@Override
	public void putNext(Tuple f) throws IOException {
		initAllFastqFieldNames();
		SequencedFragment fastqrec = tupleToSequencedFragment(f,
				allFastqFieldNames);

		try {
			writer.write(null, fastqrec);
		} catch (InterruptedException e) {
			throw new IOException(e);
		}
	}

	/**
	 * @throws IOException
	 */
	private void initAllFastqFieldNames() throws IOException {
		if (allFastqFieldNames == null) {
			try {
				// BASE64Decoder decode = new BASE64Decoder();
				Base64 codec = new Base64();
				Properties p = UDFContext.getUDFContext().getUDFProperties(
						this.getClass());
				String datastr = p.getProperty("allFastqFieldNames");
				// byte[] buffer = decode.decodeBuffer(datastr);
				byte[] buffer = codec.decodeBase64(datastr);
				ByteArrayInputStream bstream = new ByteArrayInputStream(buffer);
				ObjectInputStream ostream = new ObjectInputStream(bstream);

				allFastqFieldNames = (HashMap<String, Integer>) ostream
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
			HashMap<String, Integer> allFastqFieldNames) throws ExecException {
		SequencedFragment fastqrec = new SequencedFragment();
		int index;

		index = getFieldIndex("instrument", allFastqFieldNames);
		if (index > -1
				&& DataType.findType(tuple.get(index)) == DataType.CHARARRAY) {
			fastqrec.setInstrument((String) tuple.get(index));
		}

		index = getFieldIndex("run_number", allFastqFieldNames);
		if (index > -1
				&& DataType.findType(tuple.get(index)) == DataType.INTEGER) {
			fastqrec.setRunNumber(((Integer) tuple.get(index)));
		}

		index = getFieldIndex("flow_cell_id", allFastqFieldNames);
		if (index > -1
				&& DataType.findType(tuple.get(index)) == DataType.CHARARRAY) {
			fastqrec.setFlowcellId((String) tuple.get(index));
		}

		index = getFieldIndex("lane", allFastqFieldNames);
		if (index > -1
				&& DataType.findType(tuple.get(index)) == DataType.INTEGER) {
			fastqrec.setLane(((Integer) tuple.get(index)));
		}

		index = getFieldIndex("tile", allFastqFieldNames);
		if (index > -1
				&& DataType.findType(tuple.get(index)) == DataType.INTEGER) {
			fastqrec.setTile(((Integer) tuple.get(index)));
		}

		index = getFieldIndex("xpos", allFastqFieldNames);
		if (index > -1
				&& DataType.findType(tuple.get(index)) == DataType.INTEGER) {
			fastqrec.setXpos(((Integer) tuple.get(index)));
		}

		index = getFieldIndex("ypos", allFastqFieldNames);
		if (index > -1
				&& DataType.findType(tuple.get(index)) == DataType.INTEGER) {
			fastqrec.setYpos(((Integer) tuple.get(index)));
		}

		index = getFieldIndex("read", allFastqFieldNames);
		if (index > -1
				&& DataType.findType(tuple.get(index)) == DataType.INTEGER) {
			fastqrec.setRead(((Integer) tuple.get(index)));
		}

		index = getFieldIndex("qc_passed", allFastqFieldNames);
		if (index > -1
				&& DataType.findType(tuple.get(index)) == DataType.BOOLEAN) {
			fastqrec.setFilterPassed(((Boolean) tuple.get(index)));
		}

		index = getFieldIndex("control_number", allFastqFieldNames);
		if (index > -1
				&& DataType.findType(tuple.get(index)) == DataType.INTEGER) {
			fastqrec.setControlNumber(((Integer) tuple.get(index)));
		}

		index = getFieldIndex("index_sequence", allFastqFieldNames);
		if (index > -1
				&& DataType.findType(tuple.get(index)) == DataType.CHARARRAY) {
			fastqrec.setIndexSequence((String) tuple.get(index));
		}

		index = getFieldIndex("sequence", allFastqFieldNames);
		if (index > -1
				&& DataType.findType(tuple.get(index)) == DataType.CHARARRAY) {
			fastqrec.setSequence(new Text((String) tuple.get(index)));
		}

		index = getFieldIndex("quality", allFastqFieldNames);
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

		allFastqFieldNames = new HashMap<String, Integer>();
		String[] fieldNames = s.fieldNames();

		for (int i = 0; i < fieldNames.length; i++) {
			// System.out.println("field: "+fieldNames[i]);
			allFastqFieldNames.put(fieldNames[i], new Integer(i));
		}

		if (!( /*
				 * allFastqFieldNames.containsKey("instrument") &&
				 * allFastqFieldNames.containsKey("run_number") &&
				 * allFastqFieldNames.containsKey("flow_cell_id") &&
				 * allFastqFieldNames.containsKey("lane") &&
				 * allFastqFieldNames.containsKey("tile") &&
				 * allFastqFieldNames.containsKey("xpos") &&
				 * allFastqFieldNames.containsKey("ypos") &&
				 * allFastqFieldNames.containsKey("read") &&
				 * allFastqFieldNames.containsKey("filter") &&
				 * allFastqFieldNames.containsKey("control_number") &&
				 * allFastqFieldNames.containsKey("index_sequence")
				 */
		allFastqFieldNames.containsKey("sequence") && allFastqFieldNames
				.containsKey("quality")))
			throw new IOException(
					"Error: Incorrect Fastq tuple-field name or compulsory field missing");

		// BASE64Encoder encode = new BASE64Encoder();
		Base64 codec = new Base64();
		Properties p = UDFContext.getUDFContext().getUDFProperties(
				this.getClass());
		String datastr;
		// p.setProperty("someproperty", "value");

		ByteArrayOutputStream bstream = new ByteArrayOutputStream();
		ObjectOutputStream ostream = new ObjectOutputStream(bstream);
		ostream.writeObject(allFastqFieldNames);
		ostream.close();
		// datastr = encode.encode(bstream.toByteArray());
		datastr = codec.encodeBase64String(bstream.toByteArray());
		p.setProperty("allFastqFieldNames", datastr); // new
														// String(bstream.toByteArray(),
														// "UTF8"));
	}

	@Override
	public OutputFormat getOutputFormat() {
		return new FastqOutputFormat();
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
	public void putRDD(RDD<Tuple> rdd, String path, JobConf conf) throws IOException {

		initAllFastqFieldNames();

		FromTupleFunction FROM_TUPLE_FUNCTION = new FromTupleFunction(
				allFastqFieldNames);
		RDD<Tuple2<Text, SequencedFragment>> rddPairs = rdd.map(
				FROM_TUPLE_FUNCTION,
				SparkUtil.<Text, SequencedFragment> getTuple2Manifest());
		PairRDDFunctions<Text, SequencedFragment> pairRDDFunctions = new PairRDDFunctions<Text, SequencedFragment>(
				rddPairs, SparkUtil.getManifest(Text.class),
				SparkUtil.getManifest(SequencedFragment.class), null);
		pairRDDFunctions.saveAsNewAPIHadoopFile(path, Text.class,
				SequencedFragment.class, FastqOutputFormat.class, conf);
	}

	private static class FromTupleFunction extends
			AbstractFunction1<Tuple, Tuple2<Text, SequencedFragment>> implements
			Serializable {

		private static Text EMPTY_TEXT = new Text();
		private HashMap<String, Integer> allFastqFieldNames;

		public FromTupleFunction(HashMap<String, Integer> allFastqFieldNames) {
			this.allFastqFieldNames = allFastqFieldNames;
		}

		public Tuple2<Text, SequencedFragment> apply(Tuple v1) {
			try {

				return new Tuple2<Text, SequencedFragment>(EMPTY_TEXT,
						tupleToSequencedFragment(v1, allFastqFieldNames));
			} catch (ExecException e) {
				e.printStackTrace();
				throw new RuntimeException(e);
			}
		}
	}
}
