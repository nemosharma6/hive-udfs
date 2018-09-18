package Category;

public class DTH {

	private int deptNbr;
	private String orderLineTypeDesc;
	private String siteTypeDesc;
	private String orderLineStatusCode;
	private String retailChannelCode;

	public DTH(int dep, String linetype, String siteType, String status , String channel) {
		deptNbr = dep;
		orderLineTypeDesc = linetype;
		siteTypeDesc = siteType;
		orderLineStatusCode = status;
		retailChannelCode = channel;
	}

	public boolean check() {
		boolean c1 = orderLineTypeDesc.equals("FLOWERS") || orderLineTypeDesc.equals("OWNED_INV")
				|| orderLineTypeDesc.equals("SERVICE_AGREEMENT") || orderLineTypeDesc.equals("DSV")
				|| orderLineTypeDesc.equals("GE_APP") || orderLineTypeDesc.equals("JEWELRY");
		boolean c2 = siteTypeDesc.equals("MAINSITE");
		boolean c3 = !orderLineStatusCode.equals("9000.400") && (orderLineStatusCode.equals("3700")
				|| orderLineStatusCode.equals("3700.100") || orderLineStatusCode.equals("3700.200"));
		boolean c4 = deptNbr != 35 && deptNbr != 45 && deptNbr != 24 && deptNbr != 25 && deptNbr != 26 && deptNbr != 30
				&& deptNbr != 59 && deptNbr != 65 && deptNbr != 73 && deptNbr != 75 && deptNbr != 78 && deptNbr != 81
				&& deptNbr != 82 && deptNbr != 83 && deptNbr != 84 && deptNbr != 87 && deptNbr != 90 && deptNbr != 99;
		boolean c5 = retailChannelCode.equals("3");
		return c1 && c2 && c3 && c4 && c5;
	}
}
