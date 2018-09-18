package Category;

public class INCLUB {

	private int deptNbr;
	private String retailChannelCode;

	public INCLUB(String channel, int dep) {
		deptNbr = dep;
		retailChannelCode = channel;
	}

	public boolean check() {
		boolean c1 = retailChannelCode.equals("2");
		boolean c2 = deptNbr != 35 && deptNbr != 45 && deptNbr != 24 && deptNbr != 25 && deptNbr != 26 && deptNbr != 30
				&& deptNbr != 59 && deptNbr != 65 && deptNbr != 73 && deptNbr != 75 && deptNbr != 78 && deptNbr != 81
				&& deptNbr != 82 && deptNbr != 83 && deptNbr != 84 && deptNbr != 87 && deptNbr != 90 && deptNbr != 99;
		return c1 && c2;
	}

}
