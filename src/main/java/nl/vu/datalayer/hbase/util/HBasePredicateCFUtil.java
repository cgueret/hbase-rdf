package nl.vu.datalayer.hbase.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import nl.vu.datalayer.hbase.connection.HBaseConnection;
import nl.vu.datalayer.hbase.schema.HBasePredicateCFSchema;
import nl.vu.datalayer.hbase.schema.IHBaseSchema;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.Value;

public class HBasePredicateCFUtil implements IHBaseUtil {

	private HBasePredicateCFSchema schema;
	private HBaseConnection con;

	public HBasePredicateCFUtil(HBaseConnection con, IHBaseSchema schema) {
		this.schema = (HBasePredicateCFSchema) schema;
		this.con = con;
	}

	public void addRow(String tableName, String key, String columnFam, String columnName, String val)
			throws IOException {
		// add triples to HBase
		HTableInterface table = con.getTable(tableName);
		Put row = new Put(Bytes.toBytes(key));
		row.add(Bytes.toBytes(columnFam.replaceAll("[^A-Za-z0-9 ]", "")),
				Bytes.toBytes(columnName.replaceAll("[^A-Za-z0-9 ]", "")), Bytes.toBytes(val));
		table.put(row);

		// store full predicate URI
		String pred;
		if (columnFam.compareTo("literal") != 0) {
			pred = columnFam;
		} else {
			pred = columnName;
		}

		System.out.println("PRED ENTRY: " + pred + " " + pred.replaceAll("[^A-Za-z0-9 ]", ""));
		table = con.getTable("predicates");
		row = new Put(Bytes.toBytes(pred.replaceAll("[^A-Za-z0-9 ]", "")));
		row.add(Bytes.toBytes("URI"), Bytes.toBytes(""), Bytes.toBytes(pred));
		table.put(row);
	}

	@Override
	public ArrayList<ArrayList<String>> getRow(String[] triplet) throws IOException {
		String URI = triplet[0];
		HTableInterface table = con.getTable(HBasePredicateCFSchema.TABLE_NAME);

		Get g = new Get(Bytes.toBytes(URI));
		Result r = table.get(g);

		ArrayList<ArrayList<String>> list = new ArrayList<ArrayList<String>>();
		List<KeyValue> rawList = r.list();

		for (Iterator<KeyValue> it = rawList.iterator(); it.hasNext();) {
			KeyValue k = it.next();
			ArrayList<String> triple = new ArrayList<String>();

			String pred = Bytes.toString(k.getFamily());
			if (pred.compareTo("literal") == 0) {
				pred = Bytes.toString(k.getQualifier());
			}
			triple.add(pred);

			String val = Bytes.toString(k.getValue());
			triple.add(val);

			list.add(triple);
		}

		return list;
	}

	@Override
	public void populateTables(ArrayList<Statement> statements) throws Exception {
		for (Iterator<Statement> iter = statements.iterator(); iter.hasNext();) {
			Statement s = iter.next();

			if (s.getObject() instanceof Resource) {
				addRow(HBasePredicateCFSchema.TABLE_NAME, s.getSubject().toString(), s.getPredicate().toString(), "", s
						.getObject().toString());
			} else {
				addRow(HBasePredicateCFSchema.TABLE_NAME, s.getSubject().toString(), "literal", s.getPredicate()
						.toString(), s.getObject().toString());
			}
		}
	}

	public String getPredicate(String pred) throws IOException {
		String URI = "";

		HTableInterface table = con.getTable("predicates");

		Get g = new Get(Bytes.toBytes(pred));
		Result r = table.get(g);

		List<KeyValue> rawList = r.list();

		for (Iterator<KeyValue> it = rawList.iterator(); it.hasNext();) {
			KeyValue k = it.next();
			URI = Bytes.toString(k.getValue());
		}

		return URI;
	}

	@Override
	public String getRawCellValue(String s, String p, String o) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ArrayList<ArrayList<Value>> getAllResults(Value[] quad) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * nl.vu.datalayer.hbase.util.IHBaseUtil#getSingleResult(org.openrdf.model
	 * .Value[], java.util.Random)
	 */
	@Override
	public Value[] getSingleResult(Value[] quad, Random randomizer) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

}
