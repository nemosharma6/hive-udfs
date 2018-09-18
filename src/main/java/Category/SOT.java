package Category;

public class SOT {

	private int deptNbr;
	private String orderLineTypeDesc;
	private String retailChannelCode;

	public SOT(int dep, String type, String channel) {
		deptNbr = dep;
		orderLineTypeDesc = type;
		retailChannelCode = channel;
	}

	public boolean check() {
		boolean c1 = deptNbr != 90;
		boolean c2 = orderLineTypeDesc.equals("SPECIAL_ORDER_TIRES");
		boolean c3 = retailChannelCode.equals("3");
		return c1 && c2 && c3;
	}

}
