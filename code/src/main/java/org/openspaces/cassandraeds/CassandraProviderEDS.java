package org.openspaces.cassandraeds;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.gigaspaces.datasource.DataIterator;
import com.gigaspaces.datasource.DataSourceException;
import com.gigaspaces.datasource.SQLDataProvider;
import com.j_spaces.core.client.SQLQuery;
import com.jolbox.bonecp.BoneCPDataSource;

public class CassandraProviderEDS implements SQLDataProvider<Object> {
	private static final Logger log=Logger.getLogger(CassandraProviderEDS.class.getName());
	private List<Class<?>> classes=new ArrayList<Class<?>>();
	private Map<String,String> idMap=new HashMap<String,String>();
	BoneCPDataSource connectionPool;
	HashSet<String> schema = new HashSet<String>();
	FieldSerializer fieldSerializer=null;

	ConcurrentHashMap<String, String> insertSQLCache = new ConcurrentHashMap<String, String> ();
	ConcurrentHashMap<String, String> updateSQLCache = new ConcurrentHashMap<String, String> ();

	public CassandraProviderEDS(BoneCPDataSource connectionPool,FieldSerializer fs,String[] classes)throws Exception{
		this.connectionPool=connectionPool;
		this.fieldSerializer=fs;
		for(String clazz:classes){
			this.classes.add(Class.forName(clazz));
		}
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
	}

	@Override
	public DataIterator<Object> initialLoad() throws DataSourceException {
		Connection cn=null;
		try {
			cn=connectionPool.getConnection();
			return DataIterators.newMultiClassIterator(classes,fieldSerializer,cn);
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
		return DataIterators.newSQLIterator(query, fieldSerializer,classes, cn);
	}


}
