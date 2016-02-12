package DistributedProject;

public class Triple<Pid,Id,Socket> {
	  private final Pid pid;
	  private final Id id;
	  private final Socket socketNum;

	  public Triple(Pid pid, Id id, Socket socketNum) 
	  {
	    this.pid = pid;
	    this.id = id;
	    this.socketNum = socketNum;
	  }

	  public Pid getPid() { return pid; }
	  public Id getId() { return id; }
	  public Socket getSocket() { return socketNum; }

}
