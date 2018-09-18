package Category;

public class CPU {

	private int deptNbr;
	private String orderLineTypeCode;
	private String orderLineStatusCode;

	public CPU(int dep, String type, String status) {
		deptNbr = dep;
		orderLineStatusCode = status;
		orderLineTypeCode = type;
	}

	public boolean check() {
		boolean c1 = orderLineTypeCode.equals("CNP");
		boolean c2 = deptNbr != 35 && deptNbr != 45 && deptNbr != 24 && deptNbr != 25 && deptNbr != 26 && deptNbr != 30
				&& deptNbr != 59 && deptNbr != 65 && deptNbr != 73 && deptNbr != 75 && deptNbr != 78 && deptNbr != 81
				&& deptNbr != 82 && deptNbr != 83 && deptNbr != 84 && deptNbr != 87 && deptNbr != 90 && deptNbr != 99;
		boolean c3 = orderLineStatusCode.equals("9000.400");

		return c1 && c2 && !c3;
	}

}
