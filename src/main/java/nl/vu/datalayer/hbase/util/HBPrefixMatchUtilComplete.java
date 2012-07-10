package nl.vu.datalayer.hbase.util;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

import nl.vu.datalayer.hbase.connection.HBaseConnection;
import nl.vu.datalayer.hbase.connection.NativeJavaConnection;
import nl.vu.datalayer.hbase.exceptions.ElementNotFoundException;
import nl.vu.datalayer.hbase.exceptions.NumericalRangeException;
import nl.vu.datalayer.hbase.id.BaseId;
import nl.vu.datalayer.hbase.id.HBaseValue;
import nl.vu.datalayer.hbase.id.TypedId;
import nl.vu.datalayer.hbase.schema.HBPrefixMatchSchema;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hdfs.util.ByteArray;
import org.apache.jcs.JCS;
import org.apache.jcs.access.exception.CacheException;
import org.openrdf.model.Literal;
import org.openrdf.model.Statement;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class that exposes operations with the tables in the PrefixMatch schema
 * 
 */
public class HBPrefixMatchUtil implements IHBaseUtil {
	// Logger instance
	protected final Logger logger = LoggerFactory.getLogger(HBPrefixMatchUtil.class);

	private HBaseConnection con;

	/**
	 * Maps the query patterns to the associated tables that resolve those
	 * patterns
	 */
	private Map<String, Integer> pattern2Table = new HashMap<String, Integer>(16);

	/**
	 * Internal use: the index of the table used for retrieval
	 */
<<<<<<< HEAD
	private int tableIndex;

=======
	private int currentTableIndex;
	
	/**
	 * Variable storing time for the overhead of id2StringMap mappings upon retrieval
	 */
	private long id2StringOverhead = 0;
	
	/**
	 * Variable storing time for the overhead of String2Id mappings 
	 */
	private long string2IdOverhead = 0;
	
	/**
	 * Internal map that stores associations between Ids in the results and the corresponding Value objects
	 */
>>>>>>> upstream/master
	private HashMap<ByteArray, Value> id2ValueMap;

	private JCS resultCache;

	private ArrayList<Value> boundElements;

	private ValueFactory valueFactory;

	private String schemaSuffix;
	
	private MessageDigest mDigest;

	private int keySize;

	private byte [] numericalElement;

	private String currentPattern;

	private HashMap<ByteArray, Integer> spocOffsetMap = new HashMap<ByteArray, Integer>();

	private boolean useCache = false;

	/**
	 * @param con
	 */
	public HBPrefixMatchUtil(HBaseConnection con, boolean useCache) {
		super();
		this.con = con;
<<<<<<< HEAD
		this.useCache = useCache;

		// Build the hash map
=======
		pattern2Table = new HashMap<String, Integer>(16);
		buildPattern2TableHashMap();
		id2ValueMap = new HashMap<ByteArray, Value>();
		quadResults = new ArrayList<ArrayList<ByteArray>>();
		boundElements = new ArrayList<Value>();
		valueFactory = new ValueFactoryImpl();
		
		Properties prop = new Properties();
		try{
			prop.load(new FileInputStream("config.properties"));
			schemaSuffix = prop.getProperty(HBPrefixMatchSchema.SUFFIX_PROPERTY, "");	
			
			if (con instanceof NativeJavaConnection){
				initTablePool((NativeJavaConnection)con);
			}
		}
		catch (IOException e) {
			//continue to use the default properties
		}
		
		try {
			mDigest = MessageDigest.getInstance("MD5");
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
	}

	private void initTablePool(NativeJavaConnection con) throws IOException {
		String []tableNames = new String[HBPrefixMatchSchema.TABLE_NAMES.length+2];
		tableNames[0] = HBPrefixMatchSchema.ID2STRING + schemaSuffix;
		tableNames[1] = HBPrefixMatchSchema.ID2STRING + schemaSuffix;
		for (int i = 0; i < HBPrefixMatchSchema.TABLE_NAMES.length; i++) {
			tableNames[i+2] = HBPrefixMatchSchema.TABLE_NAMES[i]+schemaSuffix;
		}
		
		con.initTables(tableNames);
	}
	
	private void buildPattern2TableHashMap(){
>>>>>>> upstream/master
		pattern2Table.put("????", HBPrefixMatchSchema.SPOC);
		pattern2Table.put("|???", HBPrefixMatchSchema.SPOC);
		pattern2Table.put("||??", HBPrefixMatchSchema.SPOC);
		pattern2Table.put("|||?", HBPrefixMatchSchema.SPOC);
		pattern2Table.put("||||", HBPrefixMatchSchema.SPOC);

		pattern2Table.put("?|??", HBPrefixMatchSchema.POCS);
		pattern2Table.put("?||?", HBPrefixMatchSchema.POCS);
		pattern2Table.put("?|||", HBPrefixMatchSchema.POCS);

		pattern2Table.put("??|?", HBPrefixMatchSchema.OSPC);
		pattern2Table.put("|?|?", HBPrefixMatchSchema.OSPC);

		pattern2Table.put("??||", HBPrefixMatchSchema.OCSP);
		pattern2Table.put("|?||", HBPrefixMatchSchema.OCSP);

		pattern2Table.put("???|", HBPrefixMatchSchema.CSPO);
		pattern2Table.put("|??|", HBPrefixMatchSchema.CSPO);
		pattern2Table.put("||?|", HBPrefixMatchSchema.CSPO);

		pattern2Table.put("?|?|", HBPrefixMatchSchema.CPSO);

		// Initialise the caches
		id2ValueMap = new HashMap<ByteArray, Value>();
		if (useCache) {
			try {
				resultCache = JCS.getInstance("results");
			} catch (CacheException e1) {
				e1.printStackTrace();
			}
		}

		// id2ValueMap = new HashMap<ByteArray, Value>();
		boundElements = new ArrayList<Value>();
		valueFactory = new ValueFactoryImpl();

		Properties prop = new Properties();
		try {
			prop.load(new FileInputStream("config.properties"));
		} catch (IOException e) {
			// continue to use the default properties
		}
		schemaSuffix = prop.getProperty(HBPrefixMatchSchema.SUFFIX_PROPERTY, "");
	}

	/**
	 * @param quad
	 * @return
	 */
	@SuppressWarnings("unchecked")
	protected ArrayList<ByteArray> getMatchingQuads(Value[] quad, byte[] startKey) {

		ArrayList<Value> cacheKey = new ArrayList<Value>();
		for (Value v : quad)
			cacheKey.add(v);

		ArrayList<ByteArray> resultsList = null;
		if (useCache)
			resultsList = (ArrayList<ByteArray>) resultCache.get(cacheKey);

		ResultScanner results = null;
		if (resultsList == null) {
			// logger.info("GET (" + quad[0] + ", " + quad[1] + ", " + quad[2] +
			// ", " + quad[3] + ")");

			resultsList = new ArrayList<ByteArray>();

			try {

				// Search for matching quads
				Filter prefixFilter = new PrefixFilter(startKey);
				Filter keyOnlyFilter = new KeyOnlyFilter();
				Filter filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL, prefixFilter, keyOnlyFilter);
				Scan scan = new Scan(startKey, filterList);
				scan.setCaching(100);

				String tableName = HBPrefixMatchSchema.TABLE_NAMES[tableIndex] + schemaSuffix;
				HTableInterface table = con.getTable(tableName);
				results = table.getScanner(scan);

				Result r = null;
				while ((r = results.next()) != null) {
					resultsList.add(new ByteArray(r.getRow()));
				}
				results.close();

				if (useCache)
					resultCache.put(cacheKey, resultsList);

			} catch (IOException e) {
				e.printStackTrace();
			} catch (CacheException e) {
				e.printStackTrace();
			} finally {
				if (results != null)
					results.close();
			}
		}

		return resultsList;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * nl.vu.datalayer.hbase.util.IHBaseUtil#getAllResults(org.openrdf.model
	 * .Value[])
	 */
	@Override
<<<<<<< HEAD
	public ArrayList<Value[]> getAllResults(Value[] quad) throws IOException {
		id2ValueMap.clear();
		boundElements.clear();

		try {
			// Build the key
			byte[] startKey = buildKey(quad);
			// If no key can be created, no quad matches the request
			if (startKey == null)
				return null;

			ArrayList<ByteArray> resultsList = getMatchingQuads(quad, startKey);

			int sizeOfInterest = HBPrefixMatchSchema.KEY_LENGTH - startKey.length;
			HTableInterface id2StringTable = con.getTable(HBPrefixMatchSchema.ID2STRING + schemaSuffix);

			ArrayList<Get> batchGets = new ArrayList<Get>();
			ArrayList<ArrayList<ByteArray>> quadResults = new ArrayList<ArrayList<ByteArray>>();
			for (ByteArray result : resultsList)
				quadResults.add(parseKey(result.getBytes(), startKey.length, sizeOfInterest, batchGets));

			Result[] id2StringResults = id2StringTable.get(batchGets);

			// update the internal mapping between ids and strings
			for (Result result : id2StringResults) {
				byte[] rowVal = result.getValue(HBPrefixMatchSchema.COLUMN_FAMILY, HBPrefixMatchSchema.COLUMN_NAME);
				byte[] rowKey = result.getRow();
				if (rowVal == null || rowKey == null) {
					// logger.error("Id not found: " + (rowKey == null ? null :
					// hexaString(rowKey)));
				} else {
					Value val = convertStringToValue(new String(rowVal));
					id2ValueMap.put(new ByteArray(rowKey), val);
				}
			}

			// Close the table
			id2StringTable.close();

			int queryElemNo = boundElements.size();
			// build the quads in SPOC order using strings
			ArrayList<Value[]> ret = new ArrayList<Value[]>();

			for (ArrayList<ByteArray> quadList : quadResults) {
				Value[] newQuadResult = new Value[] { null, null, null, null };

				// fill in unbound elements
				for (int i = 0; i < quadList.size(); i++) {
					if ((i + queryElemNo) == HBPrefixMatchSchema.ORDER[tableIndex][2]
							&& TypedId.getType(quadList.get(i).getBytes()[0]) == TypedId.NUMERICAL) {
						// handle numerical
						TypedId id = new TypedId(quadList.get(i).getBytes());
						newQuadResult[2] = id.toLiteral();
					} else {
						int pos = HBPrefixMatchSchema.TO_SPOC_ORDER[tableIndex][(i + queryElemNo)];
						newQuadResult[pos] = id2ValueMap.get(quadList.get(i));
					}
				}

				// fill in bound elements
				for (int i = 0, j = 0; i < newQuadResult.length && j < boundElements.size(); i++) {
					if (newQuadResult[i] == null) {
						newQuadResult[i] = boundElements.get(j++);
					}
				}

				ret.add(newQuadResult);
			}

			return ret;

=======
	public ArrayList<ArrayList<Value>> getResults(Value[] quad)
			throws IOException {//we expect the pattern in the order SPOC	
		try {
			retrievalInit();
			
			long start = System.currentTimeMillis();
			byte[] startKey = buildRangeScanKeyFromQuad(quad);
			long keyBuildOverhead = System.currentTimeMillis()-start;

			long startSearch = System.currentTimeMillis();
			ArrayList<Get> batchIdGets = doRangeScan(startKey);
			long searchTime = System.currentTimeMillis() - startSearch;

			start = System.currentTimeMillis();
			Result[] id2StringResults = doBatchId2String(batchIdGets);
			id2StringOverhead = System.currentTimeMillis()-start;
			
			System.out.println("Search time: "+searchTime+"; Id2StringOverhead: "+id2StringOverhead+"; String2IdOverhead: "
									+string2IdOverhead+"; KeyBuildOverhead: "+keyBuildOverhead);
			
			updateId2ValueMap(id2StringResults);			
			
			return buildSPOCOrderResults();
		
>>>>>>> upstream/master
		} catch (NumericalRangeException e) {
			System.err.println("Bound variable numerical not in expected range: "+e.getMessage());
			return null;
		} catch (ElementNotFoundException e) {
			System.err.println(e.getMessage());
			return null;
		}
	}

<<<<<<< HEAD
	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * nl.vu.datalayer.hbase.util.IHBaseUtil#getSingleResult(org.openrdf.model
	 * .Value[], java.util.Random)
	 */
	@Override
	public Value[] getSingleResult(Value[] quad, Random randomizer) throws IOException {
		boundElements.clear();
		id2ValueMap.clear();

		try {
			// Build the key
			byte[] startKey = buildKey(quad);
			// If no key can be created, no quad matches the request
			if (startKey == null)
				return null;

			ArrayList<ByteArray> resultsList = getMatchingQuads(quad, startKey);

			// If the list is empty, return null
			if (resultsList.size() == 0) {
				return null;
			}

			// Pick a result at random
			ByteArray result = resultsList.get(randomizer.nextInt(resultsList.size()));

			// Prepare to decode it
			List<Get> batchGets = new ArrayList<Get>();
			int sizeOfInterest = HBPrefixMatchSchema.KEY_LENGTH - startKey.length;
			HTableInterface id2StringTable = con.getTable(HBPrefixMatchSchema.ID2STRING + schemaSuffix);
			ArrayList<ByteArray> quadResult = parseKey(result.getBytes(), startKey.length, sizeOfInterest, batchGets);

			// update the internal mapping between ids and strings
			for (Result id2StringResult : id2StringTable.get(batchGets)) {
				byte[] rowVal = id2StringResult.getValue(HBPrefixMatchSchema.COLUMN_FAMILY,
						HBPrefixMatchSchema.COLUMN_NAME);
				byte[] rowKey = id2StringResult.getRow();
				if (rowVal == null || rowKey == null) {
					// logger.error("Id not found: " + (rowKey == null ? null :
					// hexaString(rowKey)));
				} else {
					Value val = convertStringToValue(new String(rowVal));
					id2ValueMap.put(new ByteArray(rowKey), val);
				}
			}

			// Close the table
			id2StringTable.close();

			int queryElemNo = boundElements.size();

			Value[] decodedQuadResult = new Value[] { null, null, null, null };

			// fill in unbound elements
			for (int i = 0; i < quadResult.size(); i++) {
				if ((i + queryElemNo) == HBPrefixMatchSchema.ORDER[tableIndex][2]
						&& TypedId.getType(quadResult.get(i).getBytes()[0]) == TypedId.NUMERICAL) {
					// handle numerical
					TypedId id = new TypedId(quadResult.get(i).getBytes());
					decodedQuadResult[2] = id.toLiteral();
				} else {
					int pos = HBPrefixMatchSchema.TO_SPOC_ORDER[tableIndex][(i + queryElemNo)];
					decodedQuadResult[pos] = id2ValueMap.get(quadResult.get(i));
				}
			}

			// fill in bound elements
			for (int i = 0, j = 0; i < decodedQuadResult.length && j < boundElements.size(); i++)
				if (decodedQuadResult[i] == null)
					decodedQuadResult[i] = boundElements.get(j++);

			return decodedQuadResult;
		} catch (NumericalRangeException e) {
			e.printStackTrace();
			return null;
=======
	final private ArrayList<ArrayList<Value>> buildSPOCOrderResults() {
		int queryElemNo = boundElements.size();
		ArrayList<ArrayList<Value>> ret = new ArrayList<ArrayList<Value>>();
		
		for (ArrayList<ByteArray> quadList : quadResults) {
			ArrayList<Value> newQuadResult = new ArrayList<Value>(4);
			newQuadResult.addAll(Arrays.asList(new Value[]{null, null, null, null}));			
			
			fillInUnboundElements(queryElemNo, quadList, newQuadResult);		
			fillInBoundElements(newQuadResult);
			
			ret.add(newQuadResult);
		}
		return ret;
	}

	final private void fillInBoundElements(ArrayList<Value> newQuadResult) {
		for (int i = 0, j = 0; i<newQuadResult.size() && j<boundElements.size(); i++) {
			if (newQuadResult.get(i) == null){
				newQuadResult.set(i, boundElements.get(j++));
			}
		}
	}

	final private void fillInUnboundElements(int queryElemNo, ArrayList<ByteArray> quadList, ArrayList<Value> newQuadResult) {
		for (int i = 0; i < quadList.size(); i++) {
			if ((i+queryElemNo) == HBPrefixMatchSchema.ORDER[currentTableIndex][2] && 
					TypedId.getType(quadList.get(i).getBytes()[0]) == TypedId.NUMERICAL){
				//handle numericals
				TypedId id = new TypedId(quadList.get(i).getBytes());
				newQuadResult.set(2, id.toLiteral());
			}
			else{
				newQuadResult.set(HBPrefixMatchSchema.TO_SPOC_ORDER[currentTableIndex][(i+queryElemNo)], 
						id2ValueMap.get(quadList.get(i)));
			}
		}
	}

	final private void updateId2ValueMap(Result[] id2StringResults) throws IOException, ElementNotFoundException {
		HBaseValue hbaseValue = new HBaseValue();
		for (Result result : id2StringResults) {
			byte []rowVal = result.getValue(HBPrefixMatchSchema.COLUMN_FAMILY, HBPrefixMatchSchema.COLUMN_NAME);
			byte []rowKey = result.getRow();
			if (rowVal == null || rowKey == null){
				throw new ElementNotFoundException("Id not found in Id2String table: "+(rowKey == null ? null : hexaString(rowKey)));
			}
			else{
				ByteArrayInputStream byteStream = new ByteArrayInputStream(rowVal);
				hbaseValue.readFields(new DataInputStream(byteStream));
				Value val = hbaseValue.getUnderlyingValue();
				id2ValueMap.put(new ByteArray(rowKey), val);
			}
		}
	}

	final private Result[] doBatchId2String(ArrayList<Get> batchIdGets) throws IOException {
		HTableInterface id2StringTable = con.getTable(HBPrefixMatchSchema.ID2STRING+schemaSuffix);
		Result []id2StringResults = id2StringTable.get(batchIdGets);
		id2StringTable.close();
		return id2StringResults;
	}

	final private ArrayList<Get> doRangeScan(byte[] startKey) throws IOException {
		Filter prefixFilter = new PrefixFilter(startKey);
		Filter keyOnlyFilter = new KeyOnlyFilter();
		Filter filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL, prefixFilter, keyOnlyFilter);
		
		Scan scan = new Scan(startKey, filterList);
		scan.setCaching(100);

		String tableName = HBPrefixMatchSchema.TABLE_NAMES[currentTableIndex]+schemaSuffix;
		System.out.println("Retrieving from table: "+tableName);
		
		HTableInterface table = con.getTable(tableName);
		ResultScanner results = table.getScanner(scan);

		ArrayList<Get> batchGets = parseRangeScanResults(startKey.length, results);
		table.close();
		return batchGets;
	}

	final private ArrayList<Get> parseRangeScanResults(int startKeyLength, ResultScanner results) throws IOException {
		Result r = null;
		int sizeOfInterest = HBPrefixMatchSchema.KEY_LENGTH - startKeyLength;

		ArrayList<Get> batchGets = new ArrayList<Get>();
		//int i=0;
		while ((r = results.next()) != null) {
			//i++;
			parseKey(r.getRow(), startKeyLength, sizeOfInterest, batchGets);
		}
		results.close();
		//System.out.println("Range scan returned: "+i+" results");
		return batchGets;
	}

	final private void retrievalInit() {
		id2ValueMap.clear();
		quadResults.clear();
		boundElements.clear();
		id2StringOverhead = 0;
		string2IdOverhead = 0;
	}
	
	/*public Value convertStringToValue(String val){
		if (val.startsWith("_:")){//bNode
			return valueFactory.createBNode(val.substring(2));
>>>>>>> upstream/master
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * nl.vu.datalayer.hbase.util.IHBaseUtil#getNumberResults(org.openrdf.model
	 * .Value[])
	 */
	@Override
	public long getNumberResults(Value[] quad) throws IOException {
		id2ValueMap.clear();
		boundElements.clear();

		try {
			// Build the key
			byte[] startKey = buildKey(quad);
			// If no key can be created, no quad matches the request
			if (startKey == null)
				return 0;

			ArrayList<ByteArray> results = getMatchingQuads(quad, startKey);
			return results.size();
		} catch (NumericalRangeException e) {
			e.printStackTrace();
			return 0;
		}
	}

	/**
	 * @param val
	 * @return
	 */
	public Value convertStringToValue(String val) {
		if (val.startsWith("_:")) {// bNode
			return valueFactory.createBNode(val.substring(2));
		} else if (val.startsWith("\"")) {// literal
			int lastDQuote = val.lastIndexOf("\"");
			if (lastDQuote == val.length() - 1) {
				return valueFactory.createLiteral(val.substring(1, val.length() - 1));
			} else {
				String label = val.substring(1, lastDQuote - 1);
				if (val.charAt(lastDQuote + 1) == '@') {// we have a language
					String language = val.substring(lastDQuote + 2);
					return valueFactory.createLiteral(label, language);
				} else if (val.charAt(lastDQuote + 1) == '^') {
					String dataType = val.substring(lastDQuote + 4, val.length() - 1);
					return valueFactory.createLiteral(label, valueFactory.createURI(dataType));
				} else
					throw new IllegalArgumentException("Literal not in proper format");
			}
		} else {// URIs
			return valueFactory.createURI(val);
		}
<<<<<<< HEAD
	}

	/**
	 * @param b
	 * @return
	 */
	public static String hexaString(byte[] b) {
=======
	}*/
	
	public static String hexaString(byte []b){
>>>>>>> upstream/master
		String ret = "";
		for (int i = 0; i < b.length; i++) {
			ret += String.format("\\x%02x", b[i]);
		}
		return ret;
	}
	
	/**
	 * @param s
	 * @return
	 * @throws IOException
	 */
	public byte []retrieveId(String s) throws IOException{
		byte []sBytes = s.getBytes();
		byte []md5Hash = mDigest.digest(sBytes);
		
		Get g = new Get(md5Hash);
		g.addColumn(HBPrefixMatchSchema.COLUMN_FAMILY, HBPrefixMatchSchema.COLUMN_NAME);
		HTableInterface table = con.getTable(HBPrefixMatchSchema.STRING2ID + schemaSuffix);
		Result r = table.get(g);
		byte[] id = r.getValue(HBPrefixMatchSchema.COLUMN_FAMILY, HBPrefixMatchSchema.COLUMN_NAME);
		if (id == null) {
			System.err.println("Id does not exist for: " + s);
		}

		return id;
	}
<<<<<<< HEAD

	/**
	 * @param key
	 * @param startIndex
	 * @param sizeOfInterest
	 * @param batchGets
	 * @throws IOException
	 */
	private ArrayList<ByteArray> parseKey(byte[] key, int startIndex, int sizeOfInterest, Collection<Get> batchGets)
			throws IOException {

		int elemNo = sizeOfInterest / BaseId.SIZE;
=======
	
	final private void parseKey(byte []key, int startIndex, int sizeOfInterest, ArrayList<Get> batchGets) throws IOException{
		
		int elemNo = sizeOfInterest/BaseId.SIZE;
>>>>>>> upstream/master
		ArrayList<ByteArray> currentQuad = new ArrayList<ByteArray>(elemNo);

		int crtIndex = startIndex;
		for (int i = 0; i < elemNo; i++) {
			int length;
<<<<<<< HEAD
			byte[] elemKey;
			if (crtIndex == HBPrefixMatchSchema.OFFSETS[tableIndex][2]) {
				// for the Object position
=======
			byte [] elemKey;
			if (crtIndex == HBPrefixMatchSchema.OFFSETS[currentTableIndex][2]){//for the Object position
>>>>>>> upstream/master
				length = TypedId.SIZE;
				if (TypedId.getType(key[crtIndex]) == TypedId.STRING) {
					elemKey = new byte[BaseId.SIZE];
					System.arraycopy(key, crtIndex + 1, elemKey, 0, BaseId.SIZE);
				} else {
					// numericals
					elemKey = new byte[TypedId.SIZE];
					System.arraycopy(key, crtIndex, elemKey, 0, TypedId.SIZE);
					crtIndex += length;
					ByteArray newElem = new ByteArray(elemKey);
					currentQuad.add(newElem);
					continue;
				}
			} else {
				// for non-Object positions
				length = BaseId.SIZE;
				elemKey = new byte[length];
				System.arraycopy(key, crtIndex, elemKey, 0, length);
			}

			ByteArray newElem = new ByteArray(elemKey);
			currentQuad.add(newElem);

			if (id2ValueMap.get(newElem) == null) {
				id2ValueMap.put(newElem, null);
				Get g = new Get(elemKey);
				g.addColumn(HBPrefixMatchSchema.COLUMN_FAMILY, HBPrefixMatchSchema.COLUMN_NAME);
				batchGets.add(g);
			}

			crtIndex += length;
		}

		return currentQuad;
	}
<<<<<<< HEAD

	/**
	 * @param quad
	 * @return
	 * @throws IOException
	 * @throws NumericalRangeException
	 */
	private byte[] buildKey(Value[] quad) throws IOException, NumericalRangeException {
		String pattern = "";
		int keySize = 0;

		ArrayList<Get> string2Ids = new ArrayList<Get>();
		ArrayList<Integer> offsets = new ArrayList<Integer>();
		byte[] numerical = null;
		for (int i = 0; i < quad.length; i++) {
			if (quad[i] == null)
				pattern += "?";
			else {
				pattern += "|";
				boundElements.add(quad[i]);

				byte[] sBytes;
				if (i != 2) {// not Object
					keySize += BaseId.SIZE;
					sBytes = quad[i].toString().getBytes();
				} else {
=======
	
	final private byte []buildRangeScanKeyFromQuad(Value []quad) throws IOException, NumericalRangeException, ElementNotFoundException
	{
		currentPattern = "";
		keySize = 0;
		spocOffsetMap.clear();
		
		ArrayList<Get> string2IdGets = buildString2IdGets(quad);	
		currentTableIndex = pattern2Table.get(currentPattern);
		
		//Query the String2Id table
		byte []key = new byte[keySize];
		if (numericalElement != null && keySize==TypedId.SIZE){//we only have a numerical in our key
			Bytes.putBytes(key, HBPrefixMatchSchema.OFFSETS[currentTableIndex][2], numericalElement, 0, numericalElement.length);
		}
		else{
			key = buildRangeScanKeyFromMappedIds(string2IdGets, key);
		}
		
		return key;
	}

	final private byte[] buildRangeScanKeyFromMappedIds(ArrayList<Get> string2IdGets, byte []key) throws ElementNotFoundException, IOException {
		long start = System.currentTimeMillis();
		HTableInterface table = con.getTable(HBPrefixMatchSchema.STRING2ID+schemaSuffix);
		Result []results = table.get(string2IdGets);
		table.close();
		string2IdOverhead += System.currentTimeMillis()-start;
		
		for (Result result : results) {

			byte[] value = result.getValue(HBPrefixMatchSchema.COLUMN_FAMILY, HBPrefixMatchSchema.COLUMN_NAME);
			if (value == null) {
				throw new ElementNotFoundException("Quad element could not be found: " + new String(result.toString()) + "\n" + (result.getRow() == null ? null : hexaString(result.getRow())));
			}

			int spocIndex = spocOffsetMap.get(new ByteArray(result.getRow()));
			int offset = HBPrefixMatchSchema.OFFSETS[currentTableIndex][spocIndex];

			if (spocIndex == 2)
				Bytes.putBytes(key, offset + 1, value, 0, value.length);
			else
				Bytes.putBytes(key, offset, value, 0, value.length);
		}	
		
		if (numericalElement != null){
			Bytes.putBytes(key, HBPrefixMatchSchema.OFFSETS[currentTableIndex][2], numericalElement, 0, numericalElement.length);
		}
		return key;
	}

	private ArrayList<Get> buildString2IdGets(Value[] quad) throws UnsupportedEncodingException, NumericalRangeException {
		ArrayList<Get> string2IdGets = new ArrayList<Get>();
		
		numericalElement = null;
		for (int i = 0; i < quad.length; i++) {
			if (quad[i] == null)
				currentPattern += "?";
			else{
				currentPattern += "|";
				boundElements.add(quad[i]);
				
				byte []sBytes;
				if (i != 2){//not Object
					keySize += BaseId.SIZE;		
					sBytes = quad[i].toString().getBytes("UTF-8");
				}
				else{//Object
>>>>>>> upstream/master
					keySize += TypedId.SIZE;
					if (quad[i] instanceof Literal) {// literal
						Literal l = (Literal) quad[i];
						if (l.getDatatype() != null) {
							TypedId id = TypedId.createNumerical(l);
							if (id != null){
								numericalElement = id.getBytes();
								continue;
							}
						}
						String lString = l.toString();
<<<<<<< HEAD
						sBytes = lString.getBytes();
					} else {
						String elem = quad[i].toString();
						sBytes = elem.getBytes();
					}
				}

				byte[] reverseString = StringIdAssoc.reverseBytes(sBytes, sBytes.length);
				Get g = new Get(reverseString);
				g.addColumn(HBPrefixMatchSchema.COLUMN_FAMILY, HBPrefixMatchSchema.COLUMN_NAME);
				string2Ids.add(g);
				offsets.add(i);
			}
		}

		tableIndex = pattern2Table.get(pattern);

		HTableInterface table = con.getTable(HBPrefixMatchSchema.STRING2ID + schemaSuffix);

		byte[] key = new byte[keySize];
		for (int i = 0; i < string2Ids.size(); i++) {
			Get get = string2Ids.get(i);

			// long start = System.currentTimeMillis();
			Result result = table.get(get);
			byte[] value = result.getValue(HBPrefixMatchSchema.COLUMN_FAMILY, HBPrefixMatchSchema.COLUMN_NAME);
			// string2IdOverhead += System.currentTimeMillis() - start;

			if (value == null) {
				// byte[] rowKey = result.getRow();
				// System.err.println("Quad element could not be found " +
				// (rowKey == null ? null : hexaString(rowKey)));
				return null;
			}

			int spocIndex = offsets.get(i);
			int offset = HBPrefixMatchSchema.OFFSETS[tableIndex][spocIndex];

			if (spocIndex == 2)
				Bytes.putBytes(key, offset + 1, value, 0, value.length);
			else
				Bytes.putBytes(key, offset, value, 0, value.length);
		}
		if (numerical != null) {
			Bytes.putBytes(key, HBPrefixMatchSchema.OFFSETS[tableIndex][2], numerical, 0, numerical.length);
		}

		return key;
=======
						sBytes = lString.getBytes("UTF-8");					
					}
					else{//bNode or URI
						sBytes = quad[i].toString().getBytes("UTF-8");
					}
				}
				
				//byte []reverseString = StringIdAssoc.reverseBytes(sBytes, sBytes.length);
				byte []md5Hash = mDigest.digest(sBytes);
				Get g = new Get(md5Hash);
				g.addColumn(HBPrefixMatchSchema.COLUMN_FAMILY, HBPrefixMatchSchema.COLUMN_NAME);
				string2IdGets.add(g);
				spocOffsetMap.put(new ByteArray(md5Hash), i);
			}
		}
		
		return string2IdGets;
>>>>>>> upstream/master
	}

	@Override
	public ArrayList<ArrayList<String>> getRow(String[] quad) {
		return null;
	}

	@Override
	public String getRawCellValue(String subject, String predicate, String object) throws IOException {
		return null;
	}

	@Override
	public void populateTables(ArrayList<Statement> statements) throws Exception {

	}
}
