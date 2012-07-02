package org.openspaces.cassandraeds;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.gigaspaces.datasource.BulkDataPersister;
import com.gigaspaces.datasource.BulkItem;
import com.gigaspaces.datasource.DataIterator;
import com.gigaspaces.datasource.DataSourceException;
import com.gigaspaces.datasource.ManagedDataSource;
import com.gigaspaces.datasource.SQLDataProvider;
import com.gigaspaces.metadata.SpaceTypeDescriptor;
import com.j_spaces.core.IGSEntry;
import com.j_spaces.core.client.SQLQuery;
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

	ConcurrentHashMap<String, String> insertSQLCache = new ConcurrentHashMap<String, String> ();
	ConcurrentHashMap<String, String> updateSQLCache = new ConcurrentHashMap<String, String> ();
	
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

	

	/**
	 * Checks to see if Cassandra is listening.
	 */
	public void init(Properties properties) throws DataSourceException {
		try{
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


	@Override
	public void executeBulk(List<BulkItem> bulkItems)
	throws DataSourceException {
		Connection con = null;
		try {
			con = connectionPool.getConnection();
			for (BulkItem bulkItem : bulkItems) {
				IGSEntry item = (IGSEntry) bulkItem.getItem();
				String clazzName = item.getClassName().replaceAll("\\.", "_");
				
				String ID=item.getFieldValue(item.getPrimaryKeyName()).toString();
				
				if(item.getFieldType(item.getPrimaryKeyName()).equals("java.lang.String")){
					ID="'"+ID+"'";
				}

				switch (bulkItem.getOperation()) {
				case BulkItem.REMOVE:
					String deleteQL = "DELETE FROM " + clazzName
					+ " WHERE KEY = " + ID;
					try {
						log.info("removing :  "+deleteQL);
						executeCQL(con, deleteQL);
					} catch (Exception e) {
						e.printStackTrace();
					}

					break;
				case BulkItem.WRITE:

					String insertQL = null;
					int fldCount = item.getFieldsNames().length;
					StringBuilder insertQLBuf=new StringBuilder();
					insertQL=insertSQLCache.get(clazzName);
					if (insertQL==null)
					{
						insertQLBuf.append( "INSERT INTO " ).append( clazzName).append(" (KEY,");
						for (int i = 0; i < fldCount ; i++) {
							insertQLBuf.append( " '").append( item.getFieldsNames()[i]).append( "',");
						}
						insertQLBuf.deleteCharAt(insertQLBuf.length()-1);
						insertSQLCache.put(clazzName,insertQLBuf.toString());
					}
					else{
						insertQLBuf.append(insertQL);
					}

					insertQLBuf.append( ") VALUES (").append( ID).append(",");
					for (int i = 0; i < fldCount; i++) {
						insertQLBuf=
							insertQLBuf.append(getValue(item.getFieldsNames()[i],item.getFieldValue(i)))
								.append(",");
					}
					insertQL = insertQLBuf.deleteCharAt(insertQLBuf.length()-1).append(")").toString();
					try {
						log.info("inserting: "+insertQL);
						executeCQL(con, insertQL);
					} catch (Exception e) {
						e.printStackTrace();
					}
					break;

				case BulkItem.UPDATE:
					fldCount = item.getFieldsNames().length;
					StringBuilder updateQL=new StringBuilder("");
					updateQL.append("UPDATE ").append(clazzName).append(" SET");
					for (int i = 0; i < fldCount ; i++) {
						updateQL.
						append(" '").
						append(item.getFieldsNames()[i]).
						append("'=").
						append(getValue(item.getFieldsNames()[i],item.getFieldsValues()[i])).
						append(",");
					}
					updateQL.deleteCharAt(updateQL.length()-1);

					updateQL.append(" WHERE KEY=").append(ID);
					try {
						log.info("updating: "+updateQL.toString());
						
						executeCQL(con, updateQL.toString());
					} catch (Exception e) {
						e.printStackTrace();
					}
					break;
				}
			}
		} catch (SQLException e1) {
			e1.printStackTrace();
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
	private String getValue(String fieldName,Object val)
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

}


