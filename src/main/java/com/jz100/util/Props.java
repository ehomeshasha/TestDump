package com.jz100.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import com.google.common.io.Closeables;

public class Props {
	
	private String propsfilename;
	
	public Props(String propsFilename) {
		this.propsfilename = propsFilename;
	}
	public Properties loadProperties(String resource) throws IOException {
		InputStream propsStream = Thread.currentThread()
				.getContextClassLoader().getResourceAsStream(resource);
		if (propsStream != null) {
			try {
				Properties properties = new Properties();
				properties.load(propsStream);
				return properties;
			} finally {
				Closeables.close(propsStream, true);
			}
		}
		return null;
	}
	public Map<String, String> getProps() { 
		Properties mainClasses;
		Map<String, String> map = new HashMap<String, String>(); 
		try {
			mainClasses = loadProperties(propsfilename);
			
			if (mainClasses == null) {
				throw new IOException("Can't load any properties file?");
			}
			String keyString = null;
			String ValueString = null; 
			for (Object key :  mainClasses.keySet()) {
				keyString = (String) key;
				ValueString = (String) mainClasses.getProperty(keyString);
				map.put(keyString, ValueString);
			}
			return map;
		} catch (IOException e) {
			e.printStackTrace();
			return map;
		}
		
    }
}
