package implementations.dm_kernel;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

public class KafkaMessageSerializer {
	public byte[] serialize(Object kafkaMessage) {
		byte[] byteStream = null;
		
		try(
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			ObjectOutputStream oos = new ObjectOutputStream(baos);
		) {
			oos.writeObject(kafkaMessage);
			byteStream = baos.toByteArray();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return byteStream;
	}
	
	public Object deserialize(byte[] byteStream) {
		Object kafkaMessage = null;
		
		try(
			ByteArrayInputStream bais = new ByteArrayInputStream(byteStream);
			ObjectInputStream ois = new ObjectInputStream(bais);
		) {
			kafkaMessage = ois.readObject();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return kafkaMessage;
	}
}
