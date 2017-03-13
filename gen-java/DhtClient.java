import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.UnknownHostException;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

public class DhtClient {

	private static String getSuperNodeClientId(String ip, int port) {
		TTransport transport = null;
		try {
			transport = new TSocket(ip, port);
			TProtocol protocol = new TBinaryProtocol(new TFramedTransport(transport));
			DhtSuperNode.Client client = new DhtSuperNode.Client(protocol);
			transport.open();
			return client.getNode();
		} catch (TException e) {
			e.printStackTrace();
		} finally {
			if (transport != null)
				transport.close();
		}
		return null;
	}

	private static String getSuperNodeClientNodeList(String ip, int port) {
		TTransport transport = null;
		try {
			transport = new TSocket(ip, port);
			TProtocol protocol = new TBinaryProtocol(new TFramedTransport(transport));
			DhtSuperNode.Client client = new DhtSuperNode.Client(protocol);
			transport.open();
			return client.getNodeList();
		} catch (TException e) {
			e.printStackTrace();
		} finally {
			if (transport != null)
				transport.close();
		}
		return null;
	}

	private static String nodeOperation(String ip, int port, OperationConstant.NodeOperations op, String fileName,
			String content) throws TException {
		TTransport transport = null;
		String nodeList = null;
		try {
			transport = new TSocket(ip, port);
			TProtocol protocol = new TBinaryProtocol(new TFramedTransport(transport));
			DhtNode.Client client = new DhtNode.Client(protocol);
			try {
				transport.open();
				switch (op) {
				case WRITE:
					System.out.println("Hopping sequence:\n" + client.write(fileName, content));
					break;
				case READ:
					System.out.println("File content & Hopping sequence:\n" + client.read(fileName));
					break;
				case GET:
					return client.getdhtstructure();
				case QUIT:
					break;
				default:
					break;
				}
			} catch (TException e) {
				e.printStackTrace();
				throw e;
			}
		} finally {
			if (transport != null)
				transport.close();
		}
		return null;
	}

	/**
	 * args[0] : Super Node server ip args[1] : Super Node server port args[2] :
	 * Type of operation read/write args[3] : File name args[4] : File content
	 **/

	public static void main(String[] args) throws UnknownHostException, NumberFormatException, TException {
		System.out.println("Starting Client...");
		String superNodeServerIp = DhtUtil.getStringFromUser(DhtUtil.superNodeIpMessage,
				DhtUtil.superNodeIpErrorMessage);
		int superNodeServerPort = DhtUtil.getNumberFromUser(DhtUtil.superNodePortMessage,
				DhtUtil.superNodePortErrorMessage);
		String nodeAddr = null;
		String[] nodeDetails = null;
		String nodeInfo = null;
		boolean loop = true;
		String[] nodeIpPort = null;
		while (loop) {
			switch (DhtUtil.getTypeOperation()) {
			case READ:
				nodeAddr = getSuperNodeClientId(superNodeServerIp, superNodeServerPort);
				nodeIpPort = nodeAddr.split(":");
				nodeOperation(nodeIpPort[0].trim(), Integer.parseInt(nodeIpPort[1].trim()),
						OperationConstant.NodeOperations.READ,
						DhtUtil.getStringFromUser(DhtUtil.fileNameMessage, DhtUtil.fileNameErrorErrorMessage), null);
				break;
			case WRITE:
				nodeAddr = getSuperNodeClientId(superNodeServerIp, superNodeServerPort);
				nodeIpPort = nodeAddr.split(":");
				String fileName = DhtUtil.getStringFromUser(DhtUtil.fileNameMessage, DhtUtil.fileNameErrorErrorMessage);
				StringBuffer content = new StringBuffer();
				BufferedReader bufferedReader = null;
				try {
					FileReader fileReader = new FileReader(fileName);
					bufferedReader = new BufferedReader(fileReader);
					String line;
					new StringBuffer();
					while ((line = bufferedReader.readLine()) != null) {
						content.append(line);
					}
				} catch (IOException e) {
					System.out.println(DhtUtil.fileNameErrorErrorMessage);
					continue;
					// e.printStackTrace();
				} finally {
					if (bufferedReader != null)
						try {
							bufferedReader.close();
						} catch (IOException e) {
							e.printStackTrace();
						}
				}
				nodeOperation(nodeIpPort[0].trim(), Integer.parseInt(nodeIpPort[1].trim()),
						OperationConstant.NodeOperations.WRITE, fileName, content.toString());
				break;
			case GET:
				nodeAddr = getSuperNodeClientNodeList(superNodeServerIp, superNodeServerPort);
				String[] nodes = nodeAddr.split(",");
				System.out.println("********");
				for (String node : nodes) {
					nodeDetails = node.split("=");
					nodeIpPort = nodeDetails[1].split(":");
					nodeInfo = nodeOperation(nodeIpPort[0].trim(), Integer.parseInt(nodeIpPort[1].trim()),
							OperationConstant.NodeOperations.GET, null, null);
					System.out.println(nodeInfo);
				}
				break;
			case QUIT:
				loop = false;
				break;
			default:
				break;
			}
			if (!loop)
				break;
		}
	}
}
