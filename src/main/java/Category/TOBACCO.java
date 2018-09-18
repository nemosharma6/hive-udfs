package Category;

public class TOBACCO {

	private int deptNbr;
	private String retailChannelCode;

	public TOBACCO(int dep , String channel) {
		deptNbr = dep;
		retailChannelCode = channel;
	}

	public boolean check() {
		boolean c1 = deptNbr == 45;
		boolean c2 = retailChannelCode.equals("2");
		return c1 && c2;
	}

}
