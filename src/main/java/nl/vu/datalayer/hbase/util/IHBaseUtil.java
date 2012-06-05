package nl.vu.datalayer.hbase.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Random;

import org.openrdf.model.Statement;
import org.openrdf.model.Value;

public interface IHBaseUtil {
	/**
	 * Retrieve parsed triple/quad
	 * 
	 * @param triple
	 *            /quad
	 * @return
	 * @throws IOException
	 */
	public ArrayList<ArrayList<String>> getRow(String[] triple) throws IOException;

	/**
	 * Solves a simple query in which un-bound variable are expected to be null
	 * 
	 * @param quad
	 * @return
	 * @throws IOException
	 */
	public ArrayList<ArrayList<Value>> getAllResults(Value[] quad) throws IOException;

	/**
	 * Return one of the quad matching the given BGP
	 * 
	 * @param quad
	 *            A BGP with unbound (null) values. We expect the pattern in the
	 *            order SPOC
	 * @param randomizer
	 *            A random number generator
	 * @return
	 * @throws IOException
	 */
	public Value[] getSingleResult(Value[] quad, Random randomizer) throws IOException;

	/**
	 * Triple with "?" elements in the unbound positions
	 * 
	 * @param triple
	 * @return
	 * @throws IOException
	 */
	public String getRawCellValue(String subject, String predicate, String object) throws IOException;

	/**
	 * Populate the database with the statements
	 * 
	 * @param statements
	 * @throws Exception
	 */
	public void populateTables(ArrayList<Statement> statements) throws Exception;
}
