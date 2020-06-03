package implementations.util;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

public class JCLConfigProperties {
	
	public static Properties get() {
		Properties properties = new Properties();
		
		try {
			properties.load(new FileInputStream("./../jcl_conf/config.properties"));
			
			return properties;
		} catch (FileNotFoundException e){					
			System.err
				.println("File not found (../jcl_conf/config.properties) !!!!!");
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return properties;		
	}
	
	public static Properties get(String path) {
		Properties properties = new Properties();
		
		try {
			properties.load(new FileInputStream(path));
			
			return properties;
		} catch (FileNotFoundException e){					
			System.err
				.println("File not found (" + path + ") !!!!!");
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return properties;		
	}
}
