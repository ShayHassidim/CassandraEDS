package org.openspaces.cassandraeds;

import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.gigaspaces.annotation.pojo.SpaceId;
import com.gigaspaces.datasource.BulkDataPersister;
import com.gigaspaces.datasource.BulkItem;
import com.gigaspaces.datasource.DataIterator;
import com.gigaspaces.datasource.DataSourceException;
import com.gigaspaces.datasource.ManagedDataSource;
import com.j_spaces.core.IGSEntry;
import com.jolbox.bonecp.BoneCPDataSource;

public class CassandraPersisterEDS implements ManagedDataSource<Object>,BulkDataPersister{
	private static final Logger log=Logger.getLogger(CassandraPersisterEDS.class.getName());
	private Map<String,String> idMap=new HashMap<String,String>();
	BoneCPDataSource connectionPool;
	FieldSerializer fieldSerializer=null;

	ConcurrentHashMap<String, String> insertSQLCache = new ConcurrentHashMap<String, String> ();
	ConcurrentHashMap<String, String> updateSQLCache = new ConcurrentHashMap<String, String> ();

	public CassandraPersisterEDS(BoneCPDataSource connectionPool,FieldSerializer fs)throws Exception{
		this.connectionPool=connectionPool;
		this.fieldSerializer=fs;
	}

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

				String ID=getId(item).toString();

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
					/**
					 * This section will dynamically create column families.  Nice for testing,
					 * not good for anything else.
					 */
					/*
					if (!schema.contains(clazzName)); {
						StringBuilder createQL =new StringBuilder(
						"CREATE COLUMNFAMILY ")
						.append(clazzName)
						.append(" (KEY 'text' PRIMARY KEY ,");
						for (int i=0;i<item.getFieldsNames().length;i++) {
							createQL.append(" '").append(item.getFieldsNames()[i]).append("' 'text'").append(",");
						}
						createQL.deleteCharAt(createQL.length()-1);
						createQL.append(") WITH comparator=text AND default_validation=text");

						try{
							executeCQL(con,createQL.toString()); 
						}
						catch (Exception e) {
							e.printStackTrace(); 
						} schema.add(clazzName);
					}
					 */

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
/*
					if (updateSQLCache.containsKey(clazzName))
					{
						updateQL.append(updateSQLCache.get(clazzName));
					}
					else
					{
						updateQL.append("UPDATE ").append(clazzName).append(" SET");

						for (int i = 0; i < fldCount ; i++) {
							updateQL.append(" '").append(item.getFieldsNames()[i]).append("'=").append(getValue(item.getFieldsNames()[i],item.getFieldsValues()[i])).append(",");
						}
						updateSQLCache.put(clazzName, updateQL.toString());
					}
*/					
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
		return null;
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
			//log.info("getValue val type:"+val.getClass().getName());
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
				return "'"+fieldSerializer.serialize(fieldName,val)+"'";
			}
		}
	}

	/**
	 * Finds the field in the supplied class which is decorated with the SpaceId annotation
	 * 
	 * @param className
	 * @return
	 */
	private String getIdFieldName(String className){
		Class<?> cls=null;
		String fieldName=idMap.get(className);
		if(fieldName==null){
			try {
				cls=Class.forName(className);
				for(Method m:cls.getMethods()){
					if(m.getAnnotation(SpaceId.class)!=null){
						fieldName=m.getName().substring(3,4).toLowerCase()+m.getName().substring(4);  //trim get/set
						break;
					}
				}
				if(fieldName==null){
					throw new RuntimeException("class "+className+" has no @SpaceId");
				}
			} catch (ClassNotFoundException e) {
				throw new RuntimeException(e);
			}
			idMap.put(className, fieldName);
		}
		return fieldName;

	}

	private Object getId(IGSEntry entry){
		if(entry==null)return null;
		String fname=getIdFieldName(entry.getClassName());
		String quote="";
		if(entry.getFieldType(fname).equals("java.lang.String"))quote="'";
		return quote+entry.getFieldValue(fname)+quote;
	}

	private void executeCQL(Connection con, String cql) throws SQLException {
		Statement statement = con.createStatement();
		statement.execute(cql);
		statement.close();
	}

}


