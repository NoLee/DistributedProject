package Testing;

@SuppressWarnings("hiding")
public class Pair<String,Integer> {

	  private final String key;
	  private final Integer value;

	  public Pair(String skey, Integer svalue) 
	  {
	    this.key = skey;
	    this.value = svalue;
	  }

	  public String getKey() { return key; }
	  public Integer getValue() { return value; }

	}
