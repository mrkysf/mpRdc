package p;

import org.apache.hadoop.io.Text;

public class Pair< U, V> {

	private Text first;

	private Text second;

	public Pair(Text first, Text second) {

		this.first = first;
		this.second = second;
	} 

	public Text getKey(){
		return this.first;
	}

	public Text getValue(){
		return this.second;
	}
	
	public void  setKey(Text first){
		 this.first = first;
	}

	public void  setValue(Text second){
		 this.second = second;
	}

}
