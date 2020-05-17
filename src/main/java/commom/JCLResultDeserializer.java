
package commom;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;

import org.apache.kafka.common.serialization.Deserializer;

import interfaces.kernel.JCL_result;;

public class JCLResultDeserializer implements Deserializer<JCL_result> {

	@Override
	public JCL_result deserialize(String topic, byte[] data) {
		ByteArrayInputStream in = new ByteArrayInputStream(data);
	    ObjectInputStream is = null;
	    JCL_result result = null;
		
	    try {
			is = new ObjectInputStream(in);
			result = (JCL_result) is.readObject();
 
		} catch (ClassNotFoundException | IOException e) {
			e.printStackTrace();
		}
	    
	    return result;
	}
	
}
