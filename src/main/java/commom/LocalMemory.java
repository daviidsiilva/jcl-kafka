package commom;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class LocalMemory extends ConcurrentHashMap<Object, Object> implements Map<Object, Object>{
	
	private static final long serialVersionUID = 5849929483392620591L;
	private static Map<Object, Object> memory;
	
	private LocalMemory() { }
	
	public static synchronized Map<Object, Object> getInstance() {
		if(LocalMemory.memory == null) {
			LocalMemory.memory = new LocalMemory();
		}
		
		return LocalMemory.memory;
	}
}
