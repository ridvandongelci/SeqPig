import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.Iterator;

import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.junit.Before;
import org.junit.Test;

import fi.aalto.seqpig.RegionHit;


public class RegionHitTest {

	RegionHit regions;
	TupleFactory tupleFact;

	@Before
	public void setup()
	{
		regions = new RegionHit("chr1:0-10,chr2:0-10,chr2:10-20,"
				+ "chr3:10-20,chr4:10-20,chr4:21-30");
		tupleFact = TupleFactory.getInstance();
	}
	
	@Test
	public void simpleRegionHitTest() throws IOException {
		Tuple chr2_1_5 = tupleFact.newTuple(3);
		chr2_1_5.set(0, "chr2");
		chr2_1_5.set(1, 1);
		chr2_1_5.set(2, 5);
		
		DataBag resultBag = regions.exec(chr2_1_5);
		assertEquals(1, resultBag.size());
		
		Tuple next = resultBag.iterator().next();
		
		assertEquals("chr2",next.get(0));
		assertEquals(0, next.get(1));
		assertEquals(10, next.get(2));
	}
	
	@Test
	public void simpleRegionHitTestMultiples() throws IOException {
		Tuple chr2_1_5 = tupleFact.newTuple(3);
		chr2_1_5.set(0, "chr2");
		chr2_1_5.set(1, 10);
		chr2_1_5.set(2, 10);
		
		DataBag resultBag = regions.exec(chr2_1_5);
		assertEquals(2, resultBag.size());
		
		Iterator<Tuple> iterator = resultBag.iterator();
		Tuple next = iterator.next();
		
		assertEquals("chr2",next.get(0));
		assertEquals(0, next.get(1));
		assertEquals(10, next.get(2));
		
		next = iterator.next();
		
		assertEquals("chr2",next.get(0));
		assertEquals(10, next.get(1));
		assertEquals(20, next.get(2));
	}

	@Test
	public void simpleRegionHitTestNone() throws IOException {
		Tuple chr2_1_5 = tupleFact.newTuple(3);
		chr2_1_5.set(0, "chr2");
		chr2_1_5.set(1, 21);
		chr2_1_5.set(2, 25);
		
		DataBag resultBag = regions.exec(chr2_1_5);
		assertEquals(0, resultBag.size());		
	}
	
	@Test
	public void simpleRegionHitTestRedo() throws IOException {
		Tuple chr2_1_5 = tupleFact.newTuple(3);
		chr2_1_5.set(0, "chr2");
		chr2_1_5.set(1, 10);
		chr2_1_5.set(2, 10);
		
		DataBag resultBag = regions.exec(chr2_1_5);
		assertEquals(2, resultBag.size());
		
		Iterator<Tuple> iterator = resultBag.iterator();
		Tuple next = iterator.next();
		
		assertEquals("chr2",next.get(0));
		assertEquals(0, next.get(1));
		assertEquals(10, next.get(2));
		
		next = iterator.next();
		
		assertEquals("chr2",next.get(0));
		assertEquals(10, next.get(1));
		assertEquals(20, next.get(2));
		
		resultBag = regions.exec(chr2_1_5);
		assertEquals(2, resultBag.size());
		
		iterator = resultBag.iterator();
		next = iterator.next();
		
		assertEquals("chr2",next.get(0));
		assertEquals(0, next.get(1));
		assertEquals(10, next.get(2));
		
		next = iterator.next();
		
		assertEquals("chr2",next.get(0));
		assertEquals(10, next.get(1));
		assertEquals(20, next.get(2));
	}
	
	@Test
	public void simpleRegionHitTestIntersection() throws IOException {
		Tuple chr2_1_5 = tupleFact.newTuple(3);
		chr2_1_5.set(0, "chr2");
		chr2_1_5.set(1, 0);
		chr2_1_5.set(2, 30);
		
		DataBag resultBag = regions.exec(chr2_1_5);
		assertEquals(2, resultBag.size());
		
		Iterator<Tuple> iterator = resultBag.iterator();
		Tuple next = iterator.next();
		
		assertEquals("chr2",next.get(0));
		assertEquals(0, next.get(1));
		assertEquals(10, next.get(2));
		
		next = iterator.next();
		
		assertEquals("chr2",next.get(0));
		assertEquals(10, next.get(1));
		assertEquals(20, next.get(2));
		
	}
}
