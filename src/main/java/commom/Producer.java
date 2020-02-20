
package commom;

public class Producer extends Thread{
	private Resource re;
	String key;
	
	public Producer(Resource re, String key){
		this.re = re;
		this.key = key;
	}
	
	public void run(){
		try {
			re.create(this.key, this.key);	        	
	    } catch (Exception e) {
	    	e.printStackTrace();
	    	System.out.println("no connection");
	    } 
	}
}
