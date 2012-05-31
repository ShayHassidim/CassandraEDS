package org.openspaces.cassandraeds;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.logging.Logger;

import com.gigaspaces.datasource.DataIterator;
import com.j_spaces.core.client.SQLQuery;

/**
 * Factories for iterators
 * 
 * @author DeWayne
 *
 */
public class DataIterators {
	private static final Logger log=Logger.getLogger(DataIterators.class.getName());

	public static DataIterator<Object> newMultiClassIterator(List<Class<?>> classes,FieldSerializer fieldSerializer,Connection cn){
		return new MultiClassIterator(classes,fieldSerializer,cn);
	}

	public static <T> DataIterator<T> newTemplateIterator(T template, FieldSerializer fieldSerializer, Connection cn){
		return new TemplateIterator<T>(template,fieldSerializer,cn);
	}

	public static <T> SQLIterator<T> newSQLIterator(SQLQuery<T> query,FieldSerializer fieldSerializer,List<Class<?>> classes,Connection cn){
		return new SQLIterator<T>(query,fieldSerializer,classes,cn);
	}

	public static class SQLIterator<T> implements DataIterator<T>{
		private Class<T> clazz;
		private List<Class<?>> classes=new ArrayList<Class<?>>();
		private ResultSet rs;
		private FieldSerializer fieldSerializer;
		private Connection conn;
		private int curIndex=0;

		@SuppressWarnings("unchecked")
		public SQLIterator(SQLQuery<T> query, FieldSerializer fieldSerializer, List<Class<?>> classes,Connection cn){
			try{
				this.fieldSerializer=fieldSerializer;
				this.conn=cn;
				this.clazz=(Class<T>)Class.forName(query.getTypeName());
				for(Class<?> c:classes){
					if(clazz.isAssignableFrom(c)){
						this.classes.add(c);
					}
				}
				this.rs=sqlquery(query);
			}
			catch(Exception e){
				if(e instanceof RuntimeException)throw (RuntimeException)e;
				else throw new RuntimeException(e);
			}
		}

		//Note: always returns pojo (fails for document type query)
		private ResultSet sqlquery(SQLQuery<T> query) {
			try {
				String where=query.getQuery();

				StringBuilder sb=new StringBuilder("select * from ");
				sb.append(classes.get(0).getName().replaceAll("\\.", "_"));
				if(where.length()>0){
					sb.append(" where ").append(query.getQuery());
				}

				Statement st=conn.createStatement();

				Object[] preparedValues = query.getParameters();
				int qindex=0;
				if (preparedValues != null) {
					for (int i = 0; i < preparedValues.length; i++) {
						//st.setObject(i+1, preparedValues[i]);
						//Should use above "setObject" (with PreparedStatement), but it doesn't work
						qindex=sb.indexOf("?",qindex);
						if(qindex==-1)break;
						sb.deleteCharAt(qindex);
						if(preparedValues[i] instanceof String)sb.insert(qindex++,"'");
						sb.insert(qindex,preparedValues[i].toString());
						qindex+=preparedValues[i].toString().length();
						if(preparedValues[i] instanceof String)sb.insert(qindex,"'");
					}
				}

				log.fine("query="+sb.toString());
				return st.executeQuery(sb.toString());
			} catch (Exception e) {
				if(e instanceof RuntimeException)throw((RuntimeException)e);
				else throw new RuntimeException(e);
			}
		}

		@Override
		public boolean hasNext() {
			try{
				if(curIndex<classes.size()-1)return false;
				return !rs.isLast();
			}catch(SQLException e){
				throw new RuntimeException(e);
			}
		}

		@Override
		public T next() {
			
			try{
				if(rs.next()){
					return (T)Util.deserializeObject(classes.get(curIndex),rs,fieldSerializer);
				}
				else{
					//find next resultset with data
					while(true){
						rs=nextResultset();
						if(rs==null)return null;
						if(!rs.next())continue;
						return (T)Util.deserializeObject(classes.get(curIndex),rs,fieldSerializer);
					}
				}
			}
			catch(SQLException e){
				throw new RuntimeException(e);
			}
			
		}

		public void remove() {
			// NOOP
		}

		public void close() {
			try{
				//if(rs!=null)rs.close();
				if(conn!=null)conn.close();
			}catch(Exception e){
			}
		}
		
		private ResultSet nextResultset(){
			curIndex++;
			if(curIndex>=classes.size())return null;
			
			try{ rs.close();}catch(Exception e){}

			try{
				Statement statement = conn.createStatement();
				ResultSet rs=statement.executeQuery("select * from "+classes.get(curIndex).getName().replaceAll("\\.", "_"));
				statement.close();
				return rs;
			}
			catch(Exception e){
				if(e instanceof RuntimeException)throw((RuntimeException)e);
				else throw new RuntimeException(e);
			}
		}
		
	}

	/**
	 * Iterates over a template.
	 * 
	 * @author DeWayne
	 *
	 * @param <T>
	 */
	public static class TemplateIterator<T> implements DataIterator<T>{
		private static final Logger log=Logger.getLogger(TemplateIterator.class.getName());
		private T template;
		private FieldSerializer fieldSerializer;
		private Connection cn;
		private ResultSet rs;

		public TemplateIterator(T template,FieldSerializer fieldSerializer,Connection cn){
			this.template=template;
			this.fieldSerializer=fieldSerializer;
			this.cn=cn;
			this.rs=query();
		}

		public boolean hasNext() {
			try{
				return !rs.isLast();
			}catch(SQLException e){
				throw new RuntimeException(e);
			}
		}

		public T next() {
			try{
				if(rs.next()){
					return (T) Util.deserializeObject(template.getClass(),rs,fieldSerializer);
				}
				else{
					return null;
				}
			}
			catch(SQLException e){
				throw new RuntimeException(e);
			}
		}

		public void remove() {
			// NOOP
		}

		public void close() {
			try{
				rs.close();
				cn.close();
			}catch(Exception e){
			}
		}
		private ResultSet query(){
			try{
				Statement statement = cn.createStatement();
				List<FieldInfo> fields=Util.getFields(template.getClass());
				List<Object> values=new ArrayList<Object>();
				Set<Integer> skip=new HashSet<Integer>();
				for(int i=0;i<fields.size();i++){
					FieldInfo f=fields.get(i);
					Object val=f.getField().get(template);
					if(val!=null)values.add(val);
					else skip.add(i);
				}
				StringBuilder sb=new StringBuilder("select * from ").
				append(template.getClass().getName().replaceAll("\\.", "_")).
				append(" where ");
				for(int i=0;i<values.size();i++){
					if(skip.contains(i))continue;
					boolean needsQuotes=(values.get(i) instanceof String);
					sb.append(fields.get(i).getField().getName()).
					append("=");
					if(needsQuotes)sb.append("'");
					sb.append(values.get(i).toString());
					if(needsQuotes)sb.append("'");
					if(i<values.size()-1)sb.append(" and ");
				}
				ResultSet rs=statement.executeQuery(sb.toString());
				statement.close();
				return rs;
			}
			catch(Exception e){
				if(e instanceof RuntimeException)throw((RuntimeException)e);
				else throw new RuntimeException(e);
			}
		}
	}

	/**
	 * Iterates over all listed classes 
	 * 
	 * @author DeWayne
	 *
	 * @param <T>
	 */
	public static class MultiClassIterator implements DataIterator<Object>{
		private static final Logger log=Logger.getLogger(MultiClassIterator.class.getName());
		private List<Class<?>> classes;
		private FieldSerializer fieldSerializer;
		private Connection cn;
		private ResultSet rs;
		private int curIndex=-1;

		public MultiClassIterator(List<Class<?>> classes,FieldSerializer fieldSerializer,Connection cn){
			this.classes=classes;
			this.fieldSerializer=fieldSerializer;
			this.cn=cn;
			this.rs=nextResultset();
		}

		public boolean hasNext() {
			try{
				if(curIndex<classes.size()-1)return false;
				return !rs.isLast();
			}catch(SQLException e){
				throw new RuntimeException(e);
			}
		}

		public Object next() {
			try{
				if(rs.next()){
					return Util.deserializeObject(classes.get(curIndex),rs,fieldSerializer);
				}
				else{
					//find next resultset with data
					while(true){
						rs=nextResultset();
						if(rs==null)return null;
						if(!rs.next())continue;
						return Util.deserializeObject(classes.get(curIndex),rs,fieldSerializer);
					}
				}
			}
			catch(SQLException e){
				throw new RuntimeException(e);
			}
		}

		public void remove() {
			//NOOP
		}

		public void close() {
			try{
				if(rs!=null)rs.close();
				if(cn!=null)cn.close();
			}catch(Exception e){
			}
		}

		private ResultSet nextResultset(){
			curIndex++;
			if(curIndex>=classes.size())return null;

			try{
				Statement statement = cn.createStatement();
				ResultSet rs=statement.executeQuery("select * from "+classes.get(curIndex).getName().replaceAll("\\.", "_"));
				statement.close();
				return rs;
			}
			catch(Exception e){
				if(e instanceof RuntimeException)throw((RuntimeException)e);
				else throw new RuntimeException(e);
			}
		}

	}

}
