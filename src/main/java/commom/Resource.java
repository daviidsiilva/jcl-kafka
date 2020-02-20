
package commom;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class Resource {
	private Map<String, String> registers;
	protected boolean finished;
		
	public Resource(){
		this.registers = new ConcurrentHashMap<String, String>();
		this.finished = false;		
	}
		
	public synchronized void create(String key, String value){
		this.registers.put(key, value);
		wakeup();
	}
	
	protected synchronized void wakeup(){
		this.notify();
	}
							
	public synchronized String read(String key) throws Exception{
		if(!this.registers.containsKey(key)) {
			if(finished==false) {
				suspend();
			}
			return null;
		} else {
			return this.registers.get(key);
		}
	}
	
	protected synchronized void suspend()throws Exception{
		wait();
	}
	
	public int getNumOfRegisters(){
		return this.registers.size();
	}
	
	public synchronized void setFinished(){
		this.finished = true;
		this.notifyAll();
	}
	
	public boolean isFinished(){
		return this.finished;
	}
	
}
