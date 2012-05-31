package org.openspaces.cassandraeds;

import java.util.logging.Logger;

import com.thoughtworks.xstream.XStream;
import com.thoughtworks.xstream.io.json.JettisonMappedXmlDriver;

public class JsonFieldSerializer implements FieldSerializer {
	private static final Logger log=Logger.getLogger(JsonFieldSerializer.class.getName());
	private XStream xs=new XStream(new JettisonMappedXmlDriver());
	
	public JsonFieldSerializer(){
		xs.setMode(XStream.NO_REFERENCES);
	}
	
	@Override
	public String serialize(String fieldName, Object obj) {
		xs.alias(fieldName,obj.getClass());
		return xs.toXML(obj);
	}

	@Override
	public <T> T deserialize(Class<T> clazz, String fieldName, String data) {
		xs.alias(fieldName,clazz);
		log.fine("deserialized:"+xs.fromXML(data).getClass().getName());
		return clazz.cast(xs.fromXML(data));
	}


}
