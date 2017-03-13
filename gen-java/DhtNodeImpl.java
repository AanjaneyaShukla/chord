import java.net.UnknownHostException;

import org.apache.thrift.TException;

public class DhtNodeImpl implements DhtNode.Iface {

	private static String fileNotFound = "The requested file does not exit. Invalid file name";

	private String writeData(int hashVal, String fileName, String content) throws UnknownHostException, TException {
		String[] nodeAddr = null;
		if (DhtNodeServer.distinctNodes.size() == 1) {
			DhtNodeServer.fileMap.put(fileName, content);
			return String.valueOf(DhtNodeServer.nodeId);
		} else if (DhtNodeServer.predecessor == hashVal) {
			nodeAddr = DhtNodeServer.distinctNodes.get(hashVal).split(":");
			return (DhtNodeServer.dhtNodeServer.nodeClientSetup(nodeAddr[0], nodeAddr[1], null,
					OperationConstant.NodeOperations.WRITE, fileName, content) + "->"
					+ String.valueOf(DhtNodeServer.nodeId));
		} else if ((DhtNodeServer.predecessor < hashVal && hashVal <= DhtNodeServer.nodeId)
				|| (DhtNodeServer.predecessor > DhtNodeServer.nodeId
						&& (hashVal > DhtNodeServer.predecessor || hashVal <= DhtNodeServer.nodeId))) {
			DhtNodeServer.fileMap.put(fileName, content);
			return String.valueOf(DhtNodeServer.nodeId);
		} else {
			nodeAddr = DhtNodeServer.getNextNode(hashVal).split(":");
			return (DhtNodeServer.dhtNodeServer.nodeClientSetup(nodeAddr[0], nodeAddr[1], null,
					OperationConstant.NodeOperations.WRITE, fileName, content) + "->"
					+ String.valueOf(DhtNodeServer.nodeId));
		}
	}

	private String readData(int hashVal, String fileName) throws TException {
		String[] nodeAddr = null;
		String fileContent = DhtNodeServer.fileMap.get(fileName);
		if (DhtNodeServer.distinctNodes.size() == 1) {
			if (fileContent == null) {
				return fileNotFound;
			}
			return fileContent + "\n" + String.valueOf(DhtNodeServer.nodeId);
		} else if (DhtNodeServer.predecessor == hashVal) {
			nodeAddr = DhtNodeServer.distinctNodes.get(hashVal).split(":");
			return (DhtNodeServer.dhtNodeServer.nodeClientSetup(nodeAddr[0], nodeAddr[1], null,
					OperationConstant.NodeOperations.READ, fileName, null) + "->"
					+ String.valueOf(DhtNodeServer.nodeId));
		} else if ((DhtNodeServer.predecessor < hashVal && hashVal <= DhtNodeServer.nodeId)
				|| (DhtNodeServer.predecessor > DhtNodeServer.nodeId
						&& (hashVal > DhtNodeServer.predecessor || hashVal <= DhtNodeServer.nodeId))) {
			if (fileContent == null) {
				return fileNotFound;
			}
			return fileContent + "\n" + String.valueOf(DhtNodeServer.nodeId);
		} else {
			nodeAddr = DhtNodeServer.getNextNode(hashVal).split(":");
			return (DhtNodeServer.dhtNodeServer.nodeClientSetup(nodeAddr[0], nodeAddr[1], null,
					OperationConstant.NodeOperations.READ, fileName, null) + "->"
					+ String.valueOf(DhtNodeServer.nodeId));
		}
	}

	@Override
	public String write(String fileName, String contents) throws TException {
		try {
			int hashVal = DhtUtil.getHashedVal(fileName, DhtNodeServer.maxSize);
			return writeData(hashVal, fileName, contents);
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public String read(String fileName) throws TException {
        int hashVal = DhtUtil.getHashedVal(fileName, DhtNodeServer.maxSize);
        return readData(hashVal, fileName);
	}

	@Override
	public void updateDHT(String nodesMap) throws TException {
		DhtNodeServer.dhtNodeServer.setFingerTable(nodesMap, false);
	}

	@Override
	public String getdhtstructure() throws TException {
		return DhtNodeServer.dhtNodeServer.toString();
	}
}
