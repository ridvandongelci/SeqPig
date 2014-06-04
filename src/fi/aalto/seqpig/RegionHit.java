package fi.aalto.seqpig;

import java.io.IOException;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.StringTokenizer;

import net.sf.picard.util.IntervalTree;
import net.sf.picard.util.IntervalTree.Node;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;




public class RegionHit extends EvalFunc<DataBag> {
	//Bag factory for creating output bags
	private BagFactory mBagFactory = BagFactory.getInstance();
	//region trees for each chromosome
	private Hashtable<String, IntervalTree<Integer>> regions = null;
	

	
	//Constructor for regions
	public RegionHit(String regionList)
	{
		//Region tree for each chromosome
		regions = new Hashtable<String, IntervalTree<Integer>>();
		// Split regions by comma
		StringTokenizer tokens = new StringTokenizer(regionList,",");
		//Add each region to tree
		int i = 1;
		while(tokens.hasMoreTokens())
		{
			String region = tokens.nextToken();
			String[] strings = region.split(":|-");
			if(strings.length==3)
			{
				//Region features
				String chrName = strings[0];
				int start = Integer.parseInt(strings[1]);
				int end = Integer.parseInt(strings[2]);
				//get region tree for the chromosome
				IntervalTree<Integer> tree = regions.get(chrName);
				// if there is no tree created create the region tree
				if(tree == null)
				{
					tree = new IntervalTree<Integer>();
					regions.put(chrName, tree);
				}
				// add inteval number to the tree
				tree.put(start, end, i);
			}
			else
			{
				// NOT A VALID  REGION FORMAT
				System.out.println("WARN: "+region);
			}
			i++;
		}
	}
	
	public DataBag exec(Tuple input) throws IOException{
		// Handle empty tuple
		if (input == null || input.size() == 0)
			return null;
		//Find the regions specific sequencing read hits
		try {
			DataBag output = mBagFactory.newDefaultBag();
			String chrname = (String) input.get(0);
			int start = ((Integer) input.get(1)).intValue();
			int end = ((Integer) input.get(2)).intValue();

			IntervalTree<Integer> intervalTree = regions.get(chrname);
			
			if(intervalTree == null)
			{
				return null;
			}
			
			Iterator<Node<Integer>> result = intervalTree.overlappers(start, end);
			while (result.hasNext()) {
				Node<Integer> interval = result.next();
				Tuple tpl = TupleFactory.getInstance().newTuple(3);
				tpl.set(0, chrname);
				tpl.set(1, interval.getStart());
				tpl.set(2, interval.getEnd());
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
