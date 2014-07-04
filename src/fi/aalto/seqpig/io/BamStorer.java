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

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import net.sf.samtools.SAMFileHeader;
import net.sf.samtools.SAMFileReader.ValidationStringency;
import net.sf.samtools.SAMRecord;
import net.sf.samtools.SAMTextHeaderCodec;
import net.sf.samtools.util.StringLineReader;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
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
import fi.tkk.ics.hadoop.bam.KeyIgnoringBAMOutputFormat;
import fi.tkk.ics.hadoop.bam.SAMRecordWritable;

public class BamStorer extends StoreFunc implements StoreSparkFunc {
	protected RecordWriter writer = null;
	protected String samfileheader = null;
	protected SAMFileHeader samfileheader_decoded = null;

	protected HashMap<String, Integer> selectedBAMAttributes = null;
	protected HashMap<String, Integer> allBAMFieldNames = null;

	public BamStorer() {
		System.out.println("WARNING: noarg BamStorer() constructor!");
		decodeSAMFileHeader();
	}

	public BamStorer(String samfileheaderfilename) {

		String str = "";
		this.samfileheader = "";

		try {
			Configuration conf = UDFContext.getUDFContext().getJobConf();

			// Samfileheader name is necessary to use it later for
			// passing it to KeyIgnoringBAMOutputWriter in JobConfig
			try {
				Properties p = UDFContext.getUDFContext().getUDFProperties(
						this.getClass());
				p.setProperty("samfileheaderfilename", samfileheaderfilename);
			} catch (Exception e) {
				System.out.println(e);
				e.printStackTrace();
			}
			// see https://issues.apache.org/jira/browse/PIG-2576
			if (conf == null || conf.get("mapred.task.id") == null) {
				// we are running on the frontend
				decodeSAMFileHeader();
				return;
			}

			URI uri = new URI(samfileheaderfilename);
			FileSystem fs = FileSystem.get(uri, conf);

			BufferedReader in = new BufferedReader(new InputStreamReader(
					fs.open(new Path(samfileheaderfilename))));

			while (true) {
				str = in.readLine();

				if (str == null)
					break;
				else
					this.samfileheader += str + "\n";
			}

			in.close();
		} catch (Exception e) {
			System.out.println("ERROR: could not read BAM header from file "
					+ samfileheaderfilename);
			System.out.println("exception was: " + e.toString());
		}

		try {
			Base64 codec = new Base64();
			Properties p = UDFContext.getUDFContext().getUDFProperties(
					this.getClass());

			ByteArrayOutputStream bstream = new ByteArrayOutputStream();
			ObjectOutputStream ostream = new ObjectOutputStream(bstream);
			ostream.writeObject(this.samfileheader);
			ostream.close();
			String datastr = codec.encodeBase64String(bstream.toByteArray());
			p.setProperty("samfileheader", datastr);
		} catch (Exception e) {
			System.out
					.println("ERROR: Unable to store SAMFileHeader in BamStorer!");
		}

		this.samfileheader_decoded = getSAMFileHeader();
	}

	protected void decodeSAMFileHeader() {
		try {
			Base64 codec = new Base64();
			Properties p = UDFContext.getUDFContext().getUDFProperties(
					this.getClass());
			String datastr;

			datastr = p.getProperty("samfileheader");
			byte[] buffer = codec.decodeBase64(datastr);
			ByteArrayInputStream bstream = new ByteArrayInputStream(buffer);
			ObjectInputStream ostream = new ObjectInputStream(bstream);

			this.samfileheader = (String) ostream.readObject();
		} catch (Exception e) {
		}

		this.samfileheader_decoded = getSAMFileHeader();
	}

	@Override
	public void putNext(Tuple f) throws IOException {

		initAttributesAndFieldNames();

		SAMRecordWritable samrecwrite = new SAMRecordWritable();
		SAMRecord samrec = TupleToSAMRecord(f, samfileheader_decoded,
				allBAMFieldNames);
		samrecwrite.set(samrec);

		try {
			writer.write(null, samrecwrite);
		} catch (InterruptedException e) {
			throw new IOException(e);
		}
	}

	private void initAttributesAndFieldNames() throws IOException {
		if (selectedBAMAttributes == null || allBAMFieldNames == null) {
			try {
				Base64 codec = new Base64();
				Properties p = UDFContext.getUDFContext().getUDFProperties(
						this.getClass());
				String datastr;

				datastr = p.getProperty("selectedBAMAttributes");
				byte[] buffer = codec.decodeBase64(datastr);
				ByteArrayInputStream bstream = new ByteArrayInputStream(buffer);
				ObjectInputStream ostream = new ObjectInputStream(bstream);

				selectedBAMAttributes = (HashMap<String, Integer>) ostream
						.readObject();

				datastr = p.getProperty("allBAMFieldNames");
				buffer = codec.decodeBase64(datastr);
				bstream = new ByteArrayInputStream(buffer);
				ostream = new ObjectInputStream(bstream);

				allBAMFieldNames = (HashMap<String, Integer>) ostream
						.readObject();
			} catch (ClassNotFoundException e) {
				throw new IOException(e);
			}
		}
	}

	private static SAMRecord TupleToSAMRecord(Tuple f,
			SAMFileHeader samfileheader_decoded,
			HashMap<String, Integer> allBAMFieldNames) throws ExecException {
		SAMRecord samrec = new SAMRecord(samfileheader_decoded);

		int index = getFieldIndex("name", allBAMFieldNames);

		if (index > -1 && DataType.findType(f.get(index)) == DataType.CHARARRAY) {
			samrec.setReadName((String) f.get(index));
		}

		index = getFieldIndex("start", allBAMFieldNames);
		if (index > -1 && DataType.findType(f.get(index)) == DataType.INTEGER) {
			samrec.setAlignmentStart(((Integer) f.get(index)).intValue());
		}

		index = getFieldIndex("read", allBAMFieldNames);
		if (index > -1 && DataType.findType(f.get(index)) == DataType.CHARARRAY) {
			samrec.setReadString((String) f.get(index));
		}

		index = getFieldIndex("cigar", allBAMFieldNames);
		if (index > -1 && DataType.findType(f.get(index)) == DataType.CHARARRAY) {
			samrec.setCigarString((String) f.get(index));
		}

		index = getFieldIndex("basequal", allBAMFieldNames);
		if (index > -1 && DataType.findType(f.get(index)) == DataType.CHARARRAY) {
			samrec.setBaseQualityString((String) f.get(index));
		}

		index = getFieldIndex("flags", allBAMFieldNames);
		if (index > -1 && DataType.findType(f.get(index)) == DataType.INTEGER) {
			samrec.setFlags(((Integer) f.get(index)).intValue());
		}

		index = getFieldIndex("insertsize", allBAMFieldNames);
		if (index > -1 && DataType.findType(f.get(index)) == DataType.INTEGER) {
			samrec.setInferredInsertSize(((Integer) f.get(index)).intValue());
		}

		index = getFieldIndex("mapqual", allBAMFieldNames);
		if (index > -1 && DataType.findType(f.get(index)) == DataType.INTEGER) {
			samrec.setMappingQuality(((Integer) f.get(index)).intValue());
		}

		index = getFieldIndex("matestart", allBAMFieldNames);
		if (index > -1 && DataType.findType(f.get(index)) == DataType.INTEGER) {
			samrec.setMateAlignmentStart(((Integer) f.get(index)).intValue());
		}

		index = getFieldIndex("materefindex", allBAMFieldNames);
		if (index > -1 && DataType.findType(f.get(index)) == DataType.INTEGER) {
			samrec.setMateReferenceIndex(((Integer) f.get(index)).intValue());
		}

		index = getFieldIndex("refindex", allBAMFieldNames);
		if (index > -1 && DataType.findType(f.get(index)) == DataType.INTEGER) {
			samrec.setReferenceIndex(((Integer) f.get(index)).intValue());
		}

		index = getFieldIndex("attributes", allBAMFieldNames);
		if (index > -1 && DataType.findType(f.get(index)) == DataType.MAP) {
			Set<Map.Entry<String, Object>> set = ((HashMap<String, Object>) f
					.get(index)).entrySet();

			for (Map.Entry<String, Object> pairs : set) {
				String attributeName = pairs.getKey();

				samrec.setAttribute(attributeName.toUpperCase(),
						pairs.getValue());
			}
		}

		samrec.hashCode(); // causes eagerDecode()
		return samrec;
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

		selectedBAMAttributes = new HashMap<String, Integer>();
		allBAMFieldNames = new HashMap<String, Integer>();
		String[] fieldNames = s.fieldNames();

		for (int i = 0; i < fieldNames.length; i++) {
			System.out.println("field: " + fieldNames[i]);
			allBAMFieldNames.put(fieldNames[i], new Integer(i));

			if (fieldNames[i].equalsIgnoreCase("RG")
					|| fieldNames[i].equalsIgnoreCase("LB")
					|| fieldNames[i].equalsIgnoreCase("PU")
					|| fieldNames[i].equalsIgnoreCase("PG")
					|| fieldNames[i].equalsIgnoreCase("AS")
					|| fieldNames[i].equalsIgnoreCase("SQ")
					|| fieldNames[i].equalsIgnoreCase("MQ")
					|| fieldNames[i].equalsIgnoreCase("NM")
					|| fieldNames[i].equalsIgnoreCase("H0")
					|| fieldNames[i].equalsIgnoreCase("H1")
					|| fieldNames[i].equalsIgnoreCase("H2")
					|| fieldNames[i].equalsIgnoreCase("UQ")
					|| fieldNames[i].equalsIgnoreCase("PQ")
					|| fieldNames[i].equalsIgnoreCase("NH")
					|| fieldNames[i].equalsIgnoreCase("IH")
					|| fieldNames[i].equalsIgnoreCase("HI")
					|| fieldNames[i].equalsIgnoreCase("MD")
					|| fieldNames[i].equalsIgnoreCase("CS")
					|| fieldNames[i].equalsIgnoreCase("CQ")
					|| fieldNames[i].equalsIgnoreCase("CM")
					|| fieldNames[i].equalsIgnoreCase("R2")
					|| fieldNames[i].equalsIgnoreCase("Q2")
					|| fieldNames[i].equalsIgnoreCase("S2")
					|| fieldNames[i].equalsIgnoreCase("CC")
					|| fieldNames[i].equalsIgnoreCase("CP")
					|| fieldNames[i].equalsIgnoreCase("SM")
					|| fieldNames[i].equalsIgnoreCase("AM")
					|| fieldNames[i].equalsIgnoreCase("MF")
					|| fieldNames[i].equalsIgnoreCase("E2")
					|| fieldNames[i].equalsIgnoreCase("U2")
					|| fieldNames[i].equalsIgnoreCase("OQ")) {

				System.out.println("selected attribute: " + fieldNames[i]
						+ " i: " + i);
				selectedBAMAttributes.put(fieldNames[i], new Integer(i));
			}
		}

		if (!(allBAMFieldNames.containsKey("name")
				&& allBAMFieldNames.containsKey("start")
				&& allBAMFieldNames.containsKey("end")
				&& allBAMFieldNames.containsKey("read")
				&& allBAMFieldNames.containsKey("cigar")
				&& allBAMFieldNames.containsKey("basequal")
				&& allBAMFieldNames.containsKey("flags")
				&& allBAMFieldNames.containsKey("insertsize")
				&& allBAMFieldNames.containsKey("mapqual")
				&& allBAMFieldNames.containsKey("matestart")
				&& allBAMFieldNames.containsKey("materefindex") && allBAMFieldNames
					.containsKey("refindex")))
			throw new IOException(
					"Error: Incorrect BAM tuple-field name or compulsory field missing");

		Base64 codec = new Base64();
		Properties p = UDFContext.getUDFContext().getUDFProperties(
				this.getClass());
		String datastr;

		ByteArrayOutputStream bstream = new ByteArrayOutputStream();
		ObjectOutputStream ostream = new ObjectOutputStream(bstream);
		ostream.writeObject(selectedBAMAttributes);
		ostream.close();
		datastr = codec.encodeBase64String(bstream.toByteArray());
		p.setProperty("selectedBAMAttributes", datastr);

		bstream = new ByteArrayOutputStream();
		ostream = new ObjectOutputStream(bstream);
		ostream.writeObject(allBAMFieldNames);
		ostream.close();
		datastr = codec.encodeBase64String(bstream.toByteArray());
		p.setProperty("allBAMFieldNames", datastr);
	}

	private SAMFileHeader getSAMFileHeader() {
		final SAMTextHeaderCodec codec = new SAMTextHeaderCodec();
		codec.setValidationStringency(ValidationStringency.SILENT);
		return codec.decode(new StringLineReader(this.samfileheader),
				"SAMFileHeader.clone");
	}

	@Override
	public OutputFormat getOutputFormat() {
		KeyIgnoringBAMOutputFormat outputFormat = new KeyIgnoringBAMOutputFormat();
		outputFormat.setSAMHeader(getSAMFileHeader());
		return outputFormat;
	}

	@Override
	public void prepareToWrite(RecordWriter writer) {
		this.writer = writer;
	}

	@Override
	public void setStoreLocation(String location, Job job) throws IOException {
		FileOutputFormat.setOutputPath(job, new Path(location));
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.apache.pig.spark.StoreSparkFunc#putRDD(org.apache.spark.rdd.RDD,
	 * java.lang.String, org.apache.hadoop.mapred.JobConf) This function can not
	 * handle the BAM Header file
	 */
	@Override
	public void putRDD(RDD<Tuple> rdd, String path, JobConf conf) {
		String samfileheaderfilename;
		String header;
		try {
			Properties p = UDFContext.getUDFContext().getUDFProperties(
					this.getClass());
			samfileheaderfilename = p.getProperty("samfileheaderfilename");
			conf.set("samfileheaderfilename", samfileheaderfilename);
			header = getSamFileHeaderFromHDFS(samfileheaderfilename, conf);
			initAttributesAndFieldNames();
		} catch (IOException e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		}

		// System.out.println("samfileheaderfilename_decoded:" + header);
		// System.out.println("allBAMFieldNames:" + allBAMFieldNames);

		FromTupleFunction FROM_TUPLE_FUNCTION = new FromTupleFunction(header,
				allBAMFieldNames);
		RDD<Tuple2<Text, SAMRecordWritable>> rddPairs = rdd.map(
				FROM_TUPLE_FUNCTION,
				SparkUtil.<Text, SAMRecordWritable> getTuple2Manifest());
		PairRDDFunctions<Text, SAMRecordWritable> pairRDDFunctions = new PairRDDFunctions<Text, SAMRecordWritable>(
				rddPairs, SparkUtil.getManifest(Text.class),
				SparkUtil.getManifest(SAMRecordWritable.class), null);
		pairRDDFunctions
				.saveAsNewAPIHadoopFile(path, Text.class, SAMRecord.class,
						KeyIgnoringBAMOutputFormatExtended.class, conf);
		// pairRDDFunctions.saveAsNewAPIHadoopFile(path,
		// SparkUtil.getManifest(KeyIgnoringBAMOutputFormatExtended.class));

	}

	private static class FromTupleFunction extends
			AbstractFunction1<Tuple, Tuple2<Text, SAMRecordWritable>> implements
			Serializable {

		HashMap<String, Integer> allBAMFieldNames;
		String samfileheader;

		public FromTupleFunction(String samfileheader,
				HashMap<String, Integer> allBAMFieldNames) {
			this.allBAMFieldNames = allBAMFieldNames;
			this.samfileheader = samfileheader;
		}

		private static Text EMPTY_TEXT = new Text();

		public Tuple2<Text, SAMRecordWritable> apply(Tuple v1) {
			try {

				final SAMTextHeaderCodec codec = new SAMTextHeaderCodec();
				codec.setValidationStringency(ValidationStringency.SILENT);
				SAMFileHeader samfileheader_decoded = codec.decode(
						new StringLineReader(this.samfileheader),
						"SAMFileHeader.clone");
				SAMRecordWritable samrecwrite = new SAMRecordWritable();
				SAMRecord samRecord = TupleToSAMRecord(v1,
						samfileheader_decoded, allBAMFieldNames);
				samrecwrite.set(samRecord);
				return new Tuple2<Text, SAMRecordWritable>(EMPTY_TEXT,
						samrecwrite);
			} catch (ExecException e) {
				e.printStackTrace();
				throw new RuntimeException(e);
			}
		}
	}

	public static String getSamFileHeaderFromHDFS(String samfileheaderfilename,
			Configuration configuration) throws IOException {
		URI uri;
		try {
			uri = new URI(samfileheaderfilename);
		} catch (URISyntaxException e) {
			e.printStackTrace();
			throw new IOException("Invalid URI", e);
		}
		// Read header file
		FileSystem fs = FileSystem.get(uri, configuration);
		BufferedReader in = new BufferedReader(new InputStreamReader(
				fs.open(new Path(samfileheaderfilename))));

		String samfileheader = "";
		String str = null;
		while (true) {
			str = in.readLine();

			if (str == null)
				break;
			else
				samfileheader += str + "\n";
		}

		in.close();
		return samfileheader;

	}

	public static class KeyIgnoringBAMOutputFormatExtended extends
			KeyIgnoringBAMOutputFormat<Text> {
		@Override
		public RecordWriter<Text, SAMRecordWritable> getRecordWriter(
				TaskAttemptContext ctx, Path out) throws IOException {
			// Get JOB conf
			Configuration configuration = ctx.getConfiguration();
			// get header file name
			String samfileheaderfilename = configuration
					.get("samfileheaderfilename");

			if (samfileheaderfilename != null) {
				// parse header from ascii filename and set it
				final SAMTextHeaderCodec codec = new SAMTextHeaderCodec();
				codec.setValidationStringency(ValidationStringency.SILENT);
				SAMFileHeader decode = codec.decode(
						new StringLineReader(BamStorer
								.getSamFileHeaderFromHDFS(
										samfileheaderfilename, configuration)),
						"SAMFileHeader.clone");
				this.setSAMHeader(decode);
			}
			return super.getRecordWriter(ctx, out);
		}
	}
}
