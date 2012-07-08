package org.openspaces.cassandraeds;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.gigaspaces.datasource.BulkDataPersister;
import com.gigaspaces.datasource.BulkItem;
import com.gigaspaces.datasource.DataIterator;
import com.gigaspaces.datasource.DataSourceException;
import com.gigaspaces.datasource.ManagedDataSource;
import com.gigaspaces.datasource.SQLDataProvider;
import com.gigaspaces.document.SpaceDocument;
import com.gigaspaces.internal.transport.EntryPacket;
import com.gigaspaces.internal.transport.IEntryPacket;
import com.gigaspaces.internal.utils.collections.ConcurrentHashSet;
import com.gigaspaces.metadata.SpaceTypeDescriptor;
import com.j_spaces.core.client.SQLQuery;
import com.j_spaces.sadapter.datasource.MirrorBulkDataItem;
import com.jolbox.bonecp.BoneCPDataSource;

/**
 * Cassandra EDS.  Constructors define usage.  2 arg constructor for mirror eds.  3 arg constructors
 * for space EDS.  Which one to use depends on whether using SpaceDocuments or POJOs.
 * 
 * @author DeWayne
 *
 */
public class CassandraEDS implements ManagedDataSource<Object>,BulkDataPersister,SQLDataProvider<Object>{
	private static final Logger log=Logger.getLogger(CassandraEDS.class.getName());
	private final BoneCPDataSource connectionPool;
	private final FieldSerializer fieldSerializer;
	private final List<Class<?>> classes=new ArrayList<Class<?>>();
	private SpaceTypeDescriptor[] doctypes;
	
	ConcurrentHashMap<String, StringBuilder> insertSQLCache = new ConcurrentHashMap<String, StringBuilder> ();
	ConcurrentHashMap<String,ConcurrentHashSet<String>> columnsAdded = new ConcurrentHashMap<String,ConcurrentHashSet<String>> ();
	
	/**
	 * Constructor for mirror EDS
	 * 
	 */
	public CassandraEDS(
			BoneCPDataSource connectionPool,
			FieldSerializer fs){
		
		this.connectionPool=connectionPool;
		this.fieldSerializer=fs;
	}
			

	/**
	 * Constructor for POJO based space EDSs
	 * 
	 */
	public CassandraEDS(
			BoneCPDataSource connectionPool,
			FieldSerializer fs,
			String[] classes)
			throws Exception
			{
				this.connectionPool=connectionPool;
				this.fieldSerializer=fs;
				for(String clazz:classes){
					this.classes.add(Class.forName(clazz));
				}
	}
	
	/**
	 * Constructor for SpaceDocument based space EDSs
	 * 
	 */
	public CassandraEDS(
			BoneCPDataSource connectionPool,
			FieldSerializer fs,
			SpaceTypeDescriptor[] doctypes)
			throws Exception
			{
				this.connectionPool=connectionPool;
				this.fieldSerializer=fs;
				this.doctypes=doctypes;
	}

	
	Field _dataPacketField ;
	/**
	 * Checks to see if Cassandra is listening.
	 */
	public void init(Properties properties) throws DataSourceException {
		try{
			_dataPacketField = MirrorBulkDataItem.class.getDeclaredField("_dataPacket");
			_dataPacketField.setAccessible(true);
			Connection test=connectionPool.getConnection();
			if(test==null){
				throw new DataSourceException("no connections available");
			}
			test.close();
		}
		catch(Exception e){
			throw new DataSourceException(e);
		}
	}
	
	@Override
	public void shutdown() throws DataSourceException {
		connectionPool.close();
	}

	void generateInsertSQL(String clazzName , StringBuilder insertQLBuf, SpaceDocument item )
	{
		insertQLBuf.append( "INSERT INTO " ).append( clazzName).append(" (KEY,");
		Set<String> propsNames = item.getProperties().keySet();
		for (Iterator<String> iterator = propsNames.iterator(); iterator.hasNext();) {
			String propName = (String) iterator.next();
			insertQLBuf.append( " '").append(propName).append( "',");
		}
		insertQLBuf.deleteCharAt(insertQLBuf.length()-1);
	}
	
	public String stackTraceToString(Throwable e) {
	    StringBuilder sb = new StringBuilder();
	    for (StackTraceElement element : e.getStackTrace()) {
	        sb.append(element.toString());
	        sb.append("\n");
	    }
	    return sb.toString();
	}


	boolean isDynamicProps(BulkItem item) throws IllegalArgumentException, IllegalAccessException
	{
		IEntryPacket ep = (IEntryPacket)_dataPacketField.get(item);
		if (ep.getDynamicProperties() == null)
			return false;
		return (ep.getDynamicProperties().size() >0);
	}
	
	Set<String> getDynamicProps(BulkItem item) throws IllegalArgumentException, IllegalAccessException
	{
		IEntryPacket ep = (IEntryPacket)_dataPacketField.get(item);
		if (ep.getDynamicProperties() == null)
			return null;
		return ep.getDynamicProperties().keySet();
	}

	void addColumn(Connection con , String familyName , String colName , String colType)
	{
		// make sure we won't add a column we already added
		if (columnsAdded.containsKey(familyName))
		{
			if (columnsAdded.get(familyName).contains(colName)) return;
		}
		String alterTableCql = "ALTER TABLE " +familyName+" ADD " + colName  + " " +colType ;
		try {
			log.info("alterTableCql :  "+alterTableCql );
			executeCQL(con, alterTableCql);
			// update columnsAdded list with newly added column
			if (columnsAdded.containsKey(familyName))
				columnsAdded.get(familyName).add(colName);
			else
			{
				ConcurrentHashSet<String> colsAddedSet = new ConcurrentHashSet<String>();
				colsAddedSet.add(colName);
				columnsAdded.put(familyName, colsAddedSet);
			}
		} catch (Exception e) {
			String error = "problem deleting: " + alterTableCql + " \n" + stackTraceToString(e);
			log.info(error);
			e.printStackTrace();
			throw new RuntimeException(error);
		}
	}
	
	@Override
	public void executeBulk(List<BulkItem> bulkItems)
	throws DataSourceException {		
		Connection con = null;
		StringBuffer batchSQL=new StringBuffer ("BEGIN BATCH ");
		try {
			con = connectionPool.getConnection();
			for (BulkItem bulkItem : bulkItems) {
				SpaceDocument item = (SpaceDocument) bulkItem.getItem();
				String clazzName = item.getTypeName().replaceAll("\\.", "_");
				
				String ID="'" + bulkItem.getIdPropertyValue().toString() +"'";
				
//				if(item.getFieldType(item.getPrimaryKeyName()).equals("java.lang.String")){
//					ID="'"+ID+"'";
//				}

				switch (bulkItem.getOperation()) {
				case BulkItem.REMOVE:
					String deleteQL = "DELETE FROM " + clazzName+ " WHERE KEY = " + ID;
					batchSQL.append(deleteQL).append(" "); 
					break;
				case BulkItem.WRITE:
					StringBuilder insertQLBuf;
					boolean cacheInsertSQL = (!isDynamicProps(bulkItem));
					// use cacheInsertSQL only with POJO without dynamic props or with a Document without dynamic props
					if (cacheInsertSQL)
					{
						insertQLBuf=insertSQLCache.get(clazzName);
						if (insertQLBuf==null)
						{
							insertQLBuf = new StringBuilder();
							generateInsertSQL(clazzName , insertQLBuf, item);
							insertSQLCache.put(clazzName,new StringBuilder(insertQLBuf));
						}
						else
						{
							insertQLBuf = new StringBuilder (insertQLBuf);
						}
					}
					else
					{
						// check if there are dynamic properties. If so alter the schema and execute the Insert SQL again
						Set<String> dynamicColumns = getDynamicProps(bulkItem);
						if (dynamicColumns!=null)
						{
							for (String coldName : dynamicColumns) {
								addColumn(con,clazzName,coldName,"varchar");
							}
						}
						
						insertQLBuf = new StringBuilder();
						generateInsertSQL(clazzName , insertQLBuf, item);
					}
					
					insertQLBuf.append( ") VALUES (").append(ID).append(",");
					
					Set<String> propsNames = item.getProperties().keySet();
					for (Iterator<String> iterator = propsNames.iterator(); iterator.hasNext();) {
						String propName = (String) iterator.next();
						insertQLBuf= insertQLBuf.append(getValue(item.getProperty(propName))).append(",");
					}
					String insertQL = insertQLBuf.deleteCharAt(insertQLBuf.length()-1).append(")").toString();
					batchSQL.append(insertQL).append(" "); 

					break;

				case BulkItem.UPDATE:
					StringBuilder updateQL=new StringBuilder("");
					updateQL.append("UPDATE ").append(clazzName).append(" SET");

					propsNames = item.getProperties().keySet();
					for (Iterator<String> iterator = propsNames.iterator(); iterator.hasNext();) {
						String propName = (String) iterator.next();
						updateQL.
						append(" '").
						append(propName).
						append("'=").
						append(getValue(item.getProperty(propName))).
						append(",");
					}
					updateQL.deleteCharAt(updateQL.length()-1);
					updateQL.append(" WHERE KEY=").append(ID);
					batchSQL.append(updateQL).append(" "); 
					break;
				}
			}
			
			try {
				log.fine("CQL:  " + batchSQL);
				executeBatchCQL(con,batchSQL);
			} catch (Exception e) {
				String error = "problem : " + batchSQL + " \n" + stackTraceToString(e);
				log.info(error);
				e.printStackTrace();
				throw new RuntimeException(error);
			}

		} catch (Exception e1) {
			e1.printStackTrace();
			String error = "problem :" + stackTraceToString(e1);
			throw new RuntimeException(error);
		}
		finally{
			try {
				con.close();
			} catch (SQLException e) {
				log.log(Level.WARNING,e.getMessage(),e);
			}
		}
	}

	@Override
	public DataIterator<Object> initialLoad() throws DataSourceException {
		Connection cn=null;
		try {
			cn=connectionPool.getConnection();

			if (doctypes==null) return DataIterators.newMultiClassIterator(classes,fieldSerializer,cn);
			else return DataIterators.newMultiDocumentIterator(doctypes,fieldSerializer,cn);
		} catch (SQLException e) {
			throw new DataSourceException(e);
		}
	}

	@Override
	public DataIterator<Object> iterator(SQLQuery<Object> query)throws DataSourceException {
		Connection cn=null;
		try{
			cn=connectionPool.getConnection();
		}
		catch(Exception e){
			if(e instanceof RuntimeException)throw (RuntimeException)e;
			else throw new RuntimeException(e);
		}
		if (doctypes==null)return DataIterators.newSQLIterator(query, fieldSerializer,(Class<?>[])classes.toArray(), cn); 
		else return DataIterators.newSQLDocumentIterator(query,fieldSerializer,doctypes,cn);
	}


	/**
	 * Gets the string value of a field.  For non-java types,
	 * uses supplied serializer.  Presumes only a single level
	 * of object nesting.
	 * 
	 * @param fieldName the field to get the value of
	 * @param val the object value of the field to be converted to string
	 * @return a java.lang.String value of the field
	 */
	private String getValue(Object val)
	{
		if (val == null)
			return "''";
		else
		{
			//Note - presumes non-collection fields
			log.fine("getValue val type:"+val.getClass().getName());
			if(val.getClass().getName().startsWith("java.lang.")){
				String str = val.toString();
				if (str.indexOf("'") > 0)
				{
					return "'" +str.replaceAll("'", "''") + "'" ;
				}
				else
					return "'" + str + "'";
			}
			else{
				return "'"+fieldSerializer.serialize(val)+"'";
			}
		}
	}


	private void executeCQL(Connection con, String cql) throws SQLException {
		Statement statement = con.createStatement();
		statement.execute(cql);
		statement.close();
	}

	private void executeBatchCQL(Connection con, StringBuffer cql) throws SQLException {
		Statement statement = con.createStatement();
		cql.append( " APPLY BATCH");
		statement.execute(cql.toString());
		statement.close();
	}
	
}