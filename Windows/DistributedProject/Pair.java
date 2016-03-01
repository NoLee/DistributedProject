package DistributedProject;

public class Pair<Key,Value> {

	  private final Key key;
	  private final Value value;

	  public Pair(Key skey, Value svalue) 
	  {
	    this.key = skey;
	    this.value = svalue;
	  }

	  public Key getKey() { return key; }
	  public Value getValue() { return value; }
	}
