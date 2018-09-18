package Category;

public class AUCTION {

	private String orderLineTypeDesc;
	private String retailChannelCode;
	private int deptNbr;
	
	public AUCTION(String type , String channel , int dept) {
		orderLineTypeDesc = type;
		retailChannelCode = channel;
		deptNbr = dept;
	}

	public boolean check() {

		boolean c1 = orderLineTypeDesc.equals("AUCTIONS");
		boolean c2 = retailChannelCode.equals("3");
		boolean c3 = deptNbr != 35 && deptNbr != 45 && deptNbr != 24 && deptNbr != 25 && deptNbr != 26 && deptNbr != 30
				&& deptNbr != 59 && deptNbr != 65 && deptNbr != 73 && deptNbr != 75 && deptNbr != 78 && deptNbr != 81
				&& deptNbr != 82 && deptNbr != 83 && deptNbr != 84 && deptNbr != 87 && deptNbr != 90 && deptNbr != 99;
		return c1 && c2 && c3;
	}

}
