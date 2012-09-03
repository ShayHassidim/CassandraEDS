package org.openspaces.cassandraeds;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
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
	final String DYNAMIC_PROP_COL_NAME = "dynamic_props";
	final String TTL_PROP_COL_NAME = "time_to_live";
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

	void generateInsertSQL(String clazzName , StringBuilder insertQLBuf, BulkItem bulkItem) throws IllegalArgumentException, IllegalAccessException
	{
		insertQLBuf.append( "INSERT INTO " ).append( clazzName).append(" (KEY,");
		//Set<String> propsNames = item.getProperties().keySet();
		
		String fixedPropsNames [] = getFixedFields(bulkItem);
		for (int i = 0; i < fixedPropsNames.length; i++) {
			insertQLBuf.append( " '").append(fixedPropsNames[i]).append( "'");
			
			if (i!=fixedPropsNames.length-1)
			{
				insertQLBuf.append( ",");
			}
		}

		if (isDynamicProps(bulkItem))
		{
			insertQLBuf.append( ",'").append(DYNAMIC_PROP_COL_NAME).append( "'");
		}

		insertQLBuf.append( ",'").append(TTL_PROP_COL_NAME).append( "'");

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

    long getLease(BulkItem item) throws IllegalArgumentException, IllegalAccessException {
        IEntryPacket ep = (IEntryPacket) _dataPacketField.get(item);
        return ep.getTTL() + new Date().getTime();
    }

	Set<String> getDynamicProps(BulkItem item) throws IllegalArgumentException, IllegalAccessException
	{
		IEntryPacket ep = (IEntryPacket)_dataPacketField.get(item);
		if (ep.getDynamicProperties() == null)
			return null;
		return ep.getDynamicProperties().keySet();
	}

    String serializeDynamicProps(BulkItem item) throws IllegalArgumentException, IllegalAccessException {
        IEntryPacket ep = (IEntryPacket) _dataPacketField.get(item);

        if (ep.getDynamicProperties() == null)
            return null;

        return fieldSerializer.serialize(ep.getDynamicProperties());
    }

    String[] getFixedFields(BulkItem item) throws IllegalArgumentException, IllegalAccessException {
        IEntryPacket ep = (IEntryPacket) _dataPacketField.get(item);
        return ep.getTypeDescriptor().getPropertiesNames();
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
			// update columnsAdded list with newly added column
			if (columnsAdded.containsKey(familyName))
				columnsAdded.get(familyName).add(colName);
			else
			{
				ConcurrentHashSet<String> colsAddedSet = new ConcurrentHashSet<String>();
				colsAddedSet.add(colName);
				columnsAdded.put(familyName, colsAddedSet);
			}
			executeCQL(con, alterTableCql);
		} catch (Exception e) {
			String error = "problem alter table: " + alterTableCql + " \n" + stackTraceToString(e);
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
							generateInsertSQL(clazzName , insertQLBuf, bulkItem);
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
						if (isDynamicProps(bulkItem)){
							addColumn(con,clazzName,DYNAMIC_PROP_COL_NAME,"varchar");
						}
						
						insertQLBuf = new StringBuilder();
						generateInsertSQL(clazzName , insertQLBuf, bulkItem);
					}
					
					insertQLBuf.append( ") VALUES (").append(ID).append(",");
					
					String fixedPropsNames [] = getFixedFields(bulkItem);
					for (int i = 0; i < fixedPropsNames.length; i++) {
						insertQLBuf= insertQLBuf.append(getValue(item.getProperty(fixedPropsNames[i]))).append(",");
					}

					// store Dynamic Props
					if (isDynamicProps(bulkItem))
					{
						String dynamicPropsStr = serializeDynamicProps(bulkItem);
						dynamicPropsStr = getValue(dynamicPropsStr);
						insertQLBuf= insertQLBuf.append(dynamicPropsStr).append(",");
					}

					// store lease time
					long ttl = getLease(bulkItem);
					String ttlStr = getValue(ttl);
					insertQLBuf= insertQLBuf.append(ttlStr);
										
					String insertQL = insertQLBuf.append(")").toString();
					batchSQL.append(insertQL).append(" "); 

					break;

				case BulkItem.UPDATE:
					StringBuilder updateQL=new StringBuilder("");
					updateQL.append("UPDATE ").append(clazzName).append(" SET");

					fixedPropsNames = getFixedFields(bulkItem);
					for (int i = 0; i < fixedPropsNames.length; i++) {						
						updateQL.
						append(" '").
						append(fixedPropsNames[i]).
						append("'=").
						append(getValue(item.getProperty(fixedPropsNames[i]))).
						append(",");

					}

					if (isDynamicProps(bulkItem)) {
						String dynamicPropsStr = serializeDynamicProps(bulkItem);
						dynamicPropsStr = getValue(dynamicPropsStr);
						
						updateQL.
						append(" '").
						append(DYNAMIC_PROP_COL_NAME).
						append("'=").
						append(dynamicPropsStr).
						append(",");
					}

					ttl = getLease(bulkItem);
					updateQL.
					append(" '").
					append(TTL_PROP_COL_NAME).
					append("'=").
					append(getValue(ttl));
					
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

			if (doctypes==null) 
				return DataIterators.newMultiClassIterator(classes,fieldSerializer,cn);
			else 
				return DataIterators.newMultiDocumentIterator(doctypes,fieldSerializer,cn);
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
		if (doctypes==null)
			return DataIterators.newSQLIterator(query, fieldSerializer,(Class<?>[])classes.toArray(), cn); 
		else 
			return DataIterators.newSQLDocumentIterator(query,fieldSerializer,doctypes,cn);
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