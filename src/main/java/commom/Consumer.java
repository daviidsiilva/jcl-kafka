
package commom;

public class Consumer extends Thread{
	
	private Resource re;
	//private LinkedList<S> parteMatriz;
	
	public Consumer(Resource re){
		this.re = re;
		//parteMatriz = new LinkedList<S>();
	}
	
	public void run(){
		try {
			String str = null;
			
			while((re.isFinished()==false)||(re.getNumOfRegisters()!=0)){
				if ((str = re.read("Hello")) != null){
					//fazer ago com o recurso que foi consumido
					if (((String)str).endsWith("100 "))
						System.err.println(str);
				}
			}
					
				
		} catch (Exception e) {
			
			e.printStackTrace();
		}
				
		//System.err.println("parte da matriz com: " + parteMatriz.size()+ " colunas");
	}

}
