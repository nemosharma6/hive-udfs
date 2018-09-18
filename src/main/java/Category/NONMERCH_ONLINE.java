package Category;

public class NONMERCH_ONLINE {

	private int deptNbr;
	private String retailChannelCode;

	public NONMERCH_ONLINE(int dep , String channel) {
		deptNbr = dep;
		retailChannelCode = channel;
	}

	public boolean check() {
		boolean c1 = deptNbr != 24 && deptNbr != 25 && deptNbr != 26 && deptNbr != 30 && deptNbr != 59 && deptNbr != 65
				&& deptNbr != 73 && deptNbr != 75 && deptNbr != 78 && deptNbr != 81 && deptNbr != 82 && deptNbr != 83
				&& deptNbr != 84 && deptNbr != 87 && deptNbr != 90 && deptNbr != 99;
		boolean c2 = retailChannelCode.equals("3");
		return c1 && c2;
	}
}
