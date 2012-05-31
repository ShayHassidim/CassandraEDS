package org.openspaces.cassandraeds;

/**
 * Implementors supply a simple means of serializing java classes to a string 
 * @author DeWayne
 *
 */
public interface FieldSerializer {
	String serialize(String fieldName,Object obj);

	<T> T deserialize(Class<T> clazz,String fieldName,String data);
}
