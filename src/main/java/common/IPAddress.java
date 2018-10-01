package common;

public class IPAddress {

	private String ip;
	
	public IPAddress(String ip) {
		this.ip = ip;
	}
	
	public String getIP() {
		return ip;
	}
	
	public boolean isValid() {
		if(ip == null || ip.isEmpty() || ip.endsWith("."))
			return false;
		String[] parts = ip.split("\\.");
		if(parts.length != 4)
			return false;
		try {
			for(String part: parts) {
				int i = Integer.parseInt(part);
				if(i < 0 || i > 255)
					return false;
			}
		}catch(NumberFormatException e) {
			return false;
		}
		return true;
	}
	
}
