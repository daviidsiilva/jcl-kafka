
package commom;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;

import org.apache.kafka.common.serialization.Serializer;

import interfaces.kernel.JCL_result;

public class JCLResultSerializer implements Serializer<JCL_result> {

	@Override
	public byte[] serialize(final String topic, final JCL_result data) {
		ByteArrayOutputStream out = new ByteArrayOutputStream();
	    ObjectOutputStream os = null;

	    try {
			os = new ObjectOutputStream(out);
			os.writeObject(data);
		} catch (IOException e) {
			System.err.println("Problem on JCLResultResourceSerializer");
			e.printStackTrace();
		}
	    
	    return out.toByteArray();
	}
	
}
