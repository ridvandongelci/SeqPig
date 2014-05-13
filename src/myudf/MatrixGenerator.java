package myudf;

import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;

public class MatrixGenerator extends EvalFunc<DataBag> {
	private BagFactory mBagFactory = BagFactory.getInstance();
	private ArrayList<RegionEntry> regionList;
	
	//Single Region
	class RegionEntry
	{
		String chrName;
		int start;
		int end;
		
		public RegionEntry(String chr, int s, int e) {
			chrName = chr;
			start = s;
			end = e;
		}
	}
	
	//Constructor for regions
	public MatrixGenerator(String regionList)
	{
		this.regionList = new ArrayList<RegionEntry>();
		StringTokenizer tokens = new StringTokenizer(regionList,",");
		while(tokens.hasMoreTokens())
		{
			String region = tokens.nextToken();
			String[] strings = region.split(":|-");
			if(strings.length==3)
			{
				String chrName = strings[0];
				int start = Integer.parseInt(strings[1]);
				int end = Integer.parseInt(strings[2]);
				this.regionList.add(new RegionEntry(chrName, start, end));
			}
			else
			{
				System.out.println("WARN: "+region);
			}
		}
	}
	
	public DataBag exec(Tuple input) throws IOException,
			org.apache.pig.backend.executionengine.ExecException {
		if (input == null || input.size() == 0)
			return null;
		try {
			DataBag output = mBagFactory.newDefaultBag();
			String chrname = (String) input.get(0);
			int pos = ((Integer) input.get(1)).intValue();
			int count = ((Long) input.get(2)).intValue();

			
			for(int i=0; i<regionList.size();i++) {
				RegionEntry reg = regionList.get(i);
				if(reg.start<= pos && reg.end>= pos && reg.chrName.equalsIgnoreCase(chrname))
				{
					Tuple tpl = TupleFactory.getInstance().newTuple(4);
					tpl.set(0,i+1);
					tpl.set(1,chrname);
					tpl.set(2,pos);
					tpl.set(3,count);
					output.add(tpl);
				}
			}

			return output;

		} catch (Exception e) {
			throw new IOException(
					"Caught exception processing input row ", e);
		}
	}

	public Schema outputSchema(Schema input) {
		try {

			Schema bagSchema = new Schema();
			bagSchema.add(new Schema.FieldSchema("id", DataType.INTEGER));
			bagSchema.add(new Schema.FieldSchema("chr", DataType.CHARARRAY));
			bagSchema.add(new Schema.FieldSchema("pos", DataType.INTEGER));
			bagSchema.add(new Schema.FieldSchema("count", DataType.INTEGER));
			return new Schema(new Schema.FieldSchema(getSchemaName(this
					.getClass().getName().toLowerCase(), input), bagSchema,
					DataType.BAG));
		} catch (Exception e) {
			return null;
		}
	}
}
