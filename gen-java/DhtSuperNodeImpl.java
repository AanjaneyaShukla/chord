import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.thrift.TException;

public class DhtSuperNodeImpl implements DhtSuperNode.Iface {

	private int MAX_SIZE;
	private boolean blockNodeAdd = false;

	public DhtSuperNodeImpl(int size) {
		MAX_SIZE = size;
	}

	private static Random rand = new Random();

	private static AtomicBoolean isFree = new AtomicBoolean(true);

	private Map<Integer, String> nodeDetails = new HashMap<>();

	private String invalidAddress = "Invalid ip pr port";
	private String invalidNode = "Node already joined or join in progress";
	public static String noAck = "NACK";
	public static String nodeLimit = "NODELIMIT";
	public static String opsStarted = "OPS_STARTED";
	private String currentNodeExec = null;

	private int getNodeIndex() {
		int random = rand.nextInt(getMaxSize());
		while (nodeDetails.containsKey(random)) {
			random = rand.nextInt(getMaxSize());
		}
		return random;
	}

	private boolean isValidInput(String ip, int port) throws InvalidAddress {
		if (ip == null && ip.isEmpty() && port > 0)
			throw new InvalidAddress(404, invalidAddress);
		return true;
	}

	@Override
	public String join(String ip, int port) throws InvalidAddress, InvalidAddress, TException {
		if (blockNodeAdd) {
			return opsStarted;
		} else if (nodeDetails.size() < MAX_SIZE) {
			if (isValidInput(ip, port) && isFree.compareAndSet(true, false)) {
				String nodeAddr = ip + ":" + port;
				if (nodeDetails.values().contains(nodeAddr)) {
					isFree.set(true);
					throw new InvalidNode(411, invalidNode);
				}
				currentNodeExec = nodeAddr;
				nodeDetails.put(getNodeIndex(), currentNodeExec);
				return nodeDetails.toString().replace('{', ' ').replace('}', ' ').trim();
			} else {
				return noAck;
			}
		} else {
			System.out.println(String.format("Node limit reached and can not add node %s : %s", ip, port));
			return nodeLimit;
		}
	}

	@Override
	public void postJoin(String ip, int port) throws InvalidNode, TException {
		String receivedNode = ip + ":" + port;
		if (currentNodeExec != null && currentNodeExec.equalsIgnoreCase(receivedNode)) {
			currentNodeExec = null;
			isFree.set(true);
		} else {
			throw new InvalidNode(410, String.format("Expected post join for the node %s but received for %s",
					currentNodeExec, receivedNode));
		}
	}

	@Override
	public String getNodeList() {
		return nodeDetails.toString().replace('{', ' ').replace('}', ' ').trim();
	}

	@Override
	public String getNode() throws TException {
		blockNodeAdd = true;
		Object[] values = nodeDetails.values().toArray();
		int val = rand.nextInt(values.length);
		return values[val].toString();
	}

	@Override
	public int getMaxSize() {
		return MAX_SIZE;
	}
}
