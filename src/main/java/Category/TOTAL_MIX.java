package Category;

public class TOTAL_MIX {
	
	private int deptNbr;
	
	public TOTAL_MIX(int dep){
		deptNbr = dep;
	}
	
	public boolean check(){
		boolean c1 = deptNbr != 35 && deptNbr != 45 && deptNbr != 24 && deptNbr != 25 && deptNbr != 26 && deptNbr != 30
				&& deptNbr != 59 && deptNbr != 65 && deptNbr != 73 && deptNbr != 75 && deptNbr != 78 && deptNbr != 81
				&& deptNbr != 82 && deptNbr != 83 && deptNbr != 84 && deptNbr != 87 && deptNbr != 90 && deptNbr != 99;
		return c1;
	}
}