package Category;

public class FUEL {

	private int deptNbr;
	private String retailChannelCode;

	public FUEL(int dep , String channel) {
		deptNbr = dep;
		retailChannelCode = channel;
	}

	public boolean check() {
		boolean c1 = deptNbr == 35;
		boolean c2 = retailChannelCode.equals("2");
		return !c1 && c2;
	}

}
