package io.uve.mapreduce.subreq;

public class SubreqParser {

	private String msg;
	private String subreqName;

	public SubreqParser(String msg) {
		this.msg = msg;
		parseSubreq();
	}

	private void parseSubreq() {
		try {
			String[] subreq_fields = msg.split("\\s+");
			subreqName = subreq_fields[2];
		} catch (Exception e) {
			subreqName = "_";
		}
	}
	
	public String getSubreqname() {
		return subreqName;
	}
}
