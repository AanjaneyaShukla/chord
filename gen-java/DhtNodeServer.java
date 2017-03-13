import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.apache.thrift.transport.TTransportFactory;

public class DhtNodeServer {

	public static String clientIp;
	public static int clientPort;

	public static String superNodeServerIp;
	public static int superNodeServerPort;
	public static Integer successor;
	public static Integer predecessor;

	public static int nodeId;
	public static int maxSize;
	public static List<Integer> fingerTable = new ArrayList<>();
	public static NavigableSet<Integer> fingerSet = new TreeSet<>();
	public static NavigableMap<Integer, String> distinctNodes = new TreeMap<>();
	public static Map<String, String> fileMap = new HashMap<>();
	public static DhtNodeServer dhtNodeServer = null;

	private DhtNodeServer() throws Exception {
		clientIp = InetAddress.getLocalHost().getHostAddress();
		init();
	}

	public static String getNextNode(int hashVal) {
		Integer candidateVal = DhtNodeServer.fingerSet.ceiling(hashVal);
		if (candidateVal == null) {
			return DhtNodeServer.distinctNodes.get(DhtNodeServer.fingerSet.first());
		} else {
			return DhtNodeServer.distinctNodes.get(candidateVal);
		}
	}

	private void setNodeId(String nodeList) {
		String[] nodes = nodeList.split(",");
		for (String node : nodes) {
			String[] nodeDetail = node.split("=");
			if (nodeDetail[1].trim().equalsIgnoreCase(clientIp + ":" + clientPort)) {
				nodeId = Integer.parseInt(nodeDetail[0].trim());
			}
		}
	}

	private void updateSuccAndPred() {
		predecessor = distinctNodes.floorKey(nodeId - 1);
		if (predecessor == null)
			predecessor = distinctNodes.lastKey();
		successor = distinctNodes.ceilingKey(nodeId + 1);
		if (successor == null)
			successor = distinctNodes.firstKey();
	}

	private void updateFingerTable() {
		fingerTable.clear();
		updateSuccAndPred();
		int sizeFingerTable = (int) Math.ceil(Math.log(DhtNodeServer.maxSize) / Math.log(2));
		for (int i = 0; i < sizeFingerTable; i++) {
			Integer key = distinctNodes.ceilingKey((int) ((nodeId + Math.pow(2, i)) % maxSize));
			if (key == null) {
				key = distinctNodes.firstKey();
			}
			fingerTable.add(key);
			fingerSet.add(key);
		}
	}

	public void setFingerTable(String nodeList, boolean isInit) throws TException {
		if (isInit)
			setNodeId(nodeList);
		String[] nodes = nodeList.split(",");
		for (String node : nodes) {
			String[] nodeDetail = node.trim().split("=");
			int currentNodeId = Integer.parseInt(nodeDetail[0].trim());
			distinctNodes.put(currentNodeId, nodeDetail[1].trim());
			String[] nodeIpPort = nodeDetail[1].split(":");
			if (isInit && currentNodeId != nodeId)
				nodeClientSetup(nodeIpPort[0].trim(), nodeIpPort[1].trim(), nodeList,
						OperationConstant.NodeOperations.UPDATE, null, null);
		}
		updateFingerTable();
	}

	private void init() throws Exception {
		try {
			String nodeList = superNodeClientSetup(OperationConstant.SuperNodeOperations.JOIN);
			if (nodeList.equals(DhtSuperNodeImpl.opsStarted)) {
				System.out.println("Client ops have started, cannot add any new nodes.");
				throw new Exception("Unable to join since client ops have started");
			} else if (nodeList.equals(DhtSuperNodeImpl.noAck)) {
				System.out.println("Super node is currently busy updating the DHT. Please try again later");
				throw new Exception("Unable to join since super node is busy");
			} else if (nodeList.equals(DhtSuperNodeImpl.nodeLimit)) {
				System.out.println("Node limit has been reached.");
				throw new Exception("Unable to join since node limit has been reached");
			}

			new Thread() {
				@Override
				public void run() {
					try {
						serverSetup();
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			}.start();
			setFingerTable(nodeList, true);
			superNodeClientSetup(OperationConstant.SuperNodeOperations.POST_JOIN);
		} catch (UnknownHostException | TException e) {
			e.printStackTrace();
			throw e;
		}
	}

	public String nodeClientSetup(String nodeServerIp, String nodeServerPort, String nodeList,
			OperationConstant.NodeOperations op, String fileName, String content) throws TException {
		TTransport transport = null;
		try {
			transport = new TSocket(nodeServerIp, Integer.parseInt(nodeServerPort));
			TProtocol protocol = new TBinaryProtocol(new TFramedTransport(transport));
			DhtNode.Client client = new DhtNode.Client(protocol);
			try {
				transport.open();
			} catch (TTransportException e) {
				e.printStackTrace();
				throw e;
			}
			switch (op) {
			case UPDATE:
				client.updateDHT(nodeList);
				break;
			case WRITE:
				return client.write(fileName, content);
			case READ:
				return client.read(fileName);
			default:
				break;
			}
		} finally {
			if (transport != null)
				transport.close();
		}
		return null;
	}

	private String superNodeClientSetup(OperationConstant.SuperNodeOperations op)
			throws UnknownHostException, TException {
		TTransport transport = null;
		try {
			transport = new TSocket(superNodeServerIp, superNodeServerPort);
			TProtocol protocol = new TBinaryProtocol(new TFramedTransport(transport));
			DhtSuperNode.Client client = new DhtSuperNode.Client(protocol);
			try {
				transport.open();
			} catch (TTransportException e) {
				e.printStackTrace();
			}
			switch (op) {
			case JOIN:
				maxSize = client.getMaxSize();
				return client.join(clientIp, clientPort);
			case POST_JOIN:
				client.postJoin(clientIp, clientPort);
				break;
			case GET_NODE:
				return client.getNode();
			default:
				break;
			}
			return null;
		} finally {
			if (transport != null)
				transport.close();
		}
	}

	private void serverSetup() throws Exception {
		TServerTransport nodeServerTransport = null;
		try {
			nodeServerTransport = new TServerSocket(clientPort);
		} catch (NumberFormatException | TTransportException e) {
			e.printStackTrace();
			throw e;
		}
		TTransportFactory factory = new TFramedTransport.Factory();
		DhtNodeImpl handler = new DhtNodeImpl();
		DhtNode.Processor<DhtNodeImpl> processor = new DhtNode.Processor<>(handler);
		TThreadPoolServer.Args arguments = new TThreadPoolServer.Args(nodeServerTransport);
		arguments.processor(processor);
		arguments.transportFactory(factory);
		TServer server = new TThreadPoolServer(arguments);
		server.serve();
	}

	@Override
	public String toString() {
		String rangekeys = null;
		if (predecessor == nodeId) {
			rangekeys = "all";
		} else {
			if (predecessor == (DhtNodeServer.maxSize - 1)) {
				rangekeys = Integer.toString((predecessor + 1) % DhtNodeServer.maxSize) + "-"
						+ Integer.toString(nodeId);
			} else if (predecessor > nodeId) {
				rangekeys = Integer.toString(predecessor + 1) + "-" + Integer.toString(DhtNodeServer.maxSize - 1)
						+ "], " + "[0" + "-" + Integer.toString(nodeId);
			} else {
				rangekeys = Integer.toString(predecessor + 1) + "-" + Integer.toString(nodeId);
			}
		}
		return "DhtNodeServer\nNodeID: " + nodeId + " Range of keys: [" + rangekeys + "] NodeIp: " + clientIp
				+ " NodePort: " + clientPort + " Predecessor: " + predecessor + " Successor: " + successor
				+ " Number of files: " + fileMap.size() + " FileList: " + fileMap + " FingerTable: " + fingerTable
				+ "\n********";
	}

	public static void main(String[] args) throws Exception {
		System.out.println("Starting Node Server...");
		clientPort = DhtUtil.getNumberFromUser(DhtUtil.nodePortMessage, DhtUtil.nodePortErrorMessage);
		superNodeServerIp = DhtUtil.getStringFromUser(DhtUtil.superNodeIpMessage, DhtUtil.superNodeIpErrorMessage);
		superNodeServerPort = DhtUtil.getNumberFromUser(DhtUtil.superNodePortMessage,
				DhtUtil.superNodePortErrorMessage);
		dhtNodeServer = new DhtNodeServer();
	}
}
