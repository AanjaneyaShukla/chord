import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Scanner;

public class DhtUtil {

	public static String superNodePortMessage = "Enter the super node server port: ";
	public static String superNodePortErrorMessage = "The entered super node server port number is incorrect. Please try again";

	public static String nodePortMessage = "Enter the node server port: ";
	public static String nodePortErrorMessage = "The entered node server port number is incorrect. Please try again";

	public static String sizeMessage = "Enter the size of cluster: ";
	public static String sizeErrorMessage = "The entered size of the cluster is incorrect. Please try again";

	public static String superNodeIpMessage = "Enter the super node server ip: ";
	public static String superNodeIpErrorMessage = "The entered super node server ip is incorrect. Please try again";

	public static String fileNameMessage = "Enter the file name: ";
	public static String fileNameErrorErrorMessage = "The entered file name is incorrect. Please try again";

	public static String fileContentMessage = "Enter the file contents: ";
	public static String fileContentErrorMessage = "The entered file contents is incorrect. Please try again";

	public static String operationType = "Enter the type of operation [0=READ, 1=WRITE, 2=DHT_STRUCTURE, 3=QUIT]: ";
	public static String operationTypeError = "The entered operation type is incorrect. Please try again";

	private static Scanner in = new Scanner(System.in);

	public static int getHashedVal(String randomKey, int maxSize) {
		MessageDigest digest = null;
		try {
			digest = MessageDigest.getInstance("MD5");
			byte[] hashedBytes = digest.digest(randomKey.getBytes("UTF-8"));
			BigInteger hash = new BigInteger(hashedBytes);
			return (hash.mod(new BigInteger(Integer.toString(maxSize))).intValue());
		} catch (NoSuchAlgorithmException | UnsupportedEncodingException e) {
			e.printStackTrace();
		}
		return -1;
	}

	public static int getNumberFromUser(String message, String errorMessage) {
		int port = -1;
		while (true) {
			try {
				System.out.print(message);
				port = Integer.parseInt(in.nextLine().trim());
				if (port < 0) {
					continue;
				}
				return port;
			} catch (NumberFormatException exp) {
				System.out.println(errorMessage);
			}
		}
	}

	public static String getStringFromUser(String message, String errorMessage) {
		String ip;
		while (true) {
			System.out.print(message);
			ip = in.nextLine().trim();
			if (ip == null && ip.isEmpty()) {
				continue;
			}
			return ip;

		}
	}

	public static OperationConstant.NodeOperations getTypeOperation() {
		int type = -1;
		while (true) {
			System.out.print(operationType);
			try {
				type = Integer.parseInt(in.nextLine());
			} catch (NumberFormatException e) {
				System.out.println("Invalid input entered. Please try again");
				continue;
			}
			switch (type) {
			case 0:
				return OperationConstant.NodeOperations.READ;
			case 1:
				return OperationConstant.NodeOperations.WRITE;
			case 2:
				return OperationConstant.NodeOperations.GET;
			case 3:
				return OperationConstant.NodeOperations.QUIT;
			default:
				System.out.println("Wrong choice!!! Try again!!!");
				continue;
			}
		}
	}
}
