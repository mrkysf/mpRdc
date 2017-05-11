package p;

public class Pair1< U, V> {

	private U first;

	private V second;

	public Pair1(U first, V second) {

		this.first = first;
		this.second = second;
	} 

	public U getKey(){
		return this.first;
	}

	public V getValue(){
		return this.second;
	}
	
	public void  setKey(U first){
		 this.first = first;
	}

	public void  setValue(V second){
		 this.second = second;
	}

}
