package net.floodlightcontroller.switchvisor.web;

public final class OperationStatus {
	protected static final class Codes {
		protected static final String OKAY = "0";
		protected static final String ALREADY_EXISTS = "1";
		protected static final String INVALID_ARGUMENT = "2";
	}
	
	private OperationStatus() {}
	
	public static final class AddProxy {
		public static final class Codes {
			
		}
	}
}
