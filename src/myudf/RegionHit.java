package myudf;

import java.io.IOException;
import java.util.Hashtable;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.gephi.data.attributes.type.Interval;
import org.gephi.data.attributes.type.IntervalTree;



public class RegionHit extends EvalFunc<DataBag> {
	private BagFactory mBagFactory = BagFactory.getInstance();
	private Hashtable<String, IntervalTree<Integer>> regions = null;
	

	
	//Constructor for regions
	public RegionHit(String regionList)
	{
		regions = new Hashtable<String, IntervalTree<Integer>>();
		StringTokenizer tokens = new StringTokenizer(regionList,",");
		int i = 1;
		while(tokens.hasMoreTokens())
		{
			String region = tokens.nextToken();
			String[] strings = region.split(":|-");
			if(strings.length==3)
			{
				String chrName = strings[0];
				int start = Integer.parseInt(strings[1]);
				int end = Integer.parseInt(strings[2]);
				//this.regionList.add(new RegionEntry(chrName, start, end));
				IntervalTree<Integer> tree = regions.get(chrName);
				if(tree == null)
				{
					tree = new IntervalTree<Integer>();
					regions.put(chrName, tree);
				}
				
				tree.insert(new Interval<Integer>(start, end, i));
			}
			else
			{
				System.out.println("WARN: "+region);
			}
			i++;
		}
	}
	
	public DataBag exec(Tuple input) throws IOException,
			org.apache.pig.backend.executionengine.ExecException {
		if (input == null || input.size() == 0)
			return null;
		try {
			DataBag output = mBagFactory.newDefaultBag();
			String chrname = (String) input.get(0);
			int start = ((Integer) input.get(1)).intValue();
			int end = ((Integer) input.get(2)).intValue();

			IntervalTree<Integer> intervalTree = regions.get(chrname);
			List<Interval<Integer>> result = intervalTree.search(start, end);
			for (Interval<Integer> interval : result) {
				Tuple tpl = TupleFactory.getInstance().newTuple(3);
				tpl.set(0, chrname);
				tpl.set(1, ((Double)interval.getLow()).intValue());
				tpl.set(2, ((Double)interval.getHigh()).intValue());
				output.add(tpl);
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
			//bagSchema.add(new Schema.FieldSchema("rank", DataType.INTEGER));
			bagSchema.add(new Schema.FieldSchema("chr", DataType.CHARARRAY));
			bagSchema.add(new Schema.FieldSchema("start", DataType.INTEGER));
			bagSchema.add(new Schema.FieldSchema("end", DataType.INTEGER));
			return new Schema(new Schema.FieldSchema(getSchemaName(this
					.getClass().getName().toLowerCase(), input), bagSchema,
					DataType.BAG));
		} catch (Exception e) {
			return null;
		}
	}
}
