package interfaces.kernel;

/**
 * @author Joubert
 * @version 1.0
 * 
 * Enables any message coded as an array of strings (not control ones). such messages are used to call servers and slaves
 */
public interface JCL_message_commons extends JCL_message{
	
	/**
	 * Returns any type of information coded as char[].
	 * @return the coded information 
	 */
	public abstract String[] getRegisterData();
	
	/**
	 * Set the message content.
	 * @param data the content coded as a char[]
	 */
	public abstract void setRegisterData(String...data);
	
	
	/**
	 * Get the Msg type of the class.
	 */
	public abstract int getMsgType();

}
