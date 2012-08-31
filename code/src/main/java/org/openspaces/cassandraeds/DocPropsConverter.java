package org.openspaces.cassandraeds;

import com.thoughtworks.xstream.converters.collections.MapConverter;
import com.thoughtworks.xstream.mapper.Mapper;

public class DocPropsConverter extends MapConverter{
	
	public DocPropsConverter(Mapper mapper) {
		super(mapper);
	}

	boolean canConvert(Object type)
	{
		if (type instanceof com.gigaspaces.document.DocumentProperties) {
			return true;
		}
		else
			return false;
			
	}
}
