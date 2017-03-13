import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.transport.TTransportFactory;

public class DhtSuperNodeServer {

	public static void main(String[] args) throws Exception {
		TServerTransport serverTransport = null;
		DhtSuperNodeImpl handler = null;
		System.out.println("Starting Super Node Server...");
		try {
			serverTransport = new TServerSocket(
					DhtUtil.getNumberFromUser(DhtUtil.superNodePortMessage, DhtUtil.superNodePortErrorMessage));
			handler = new DhtSuperNodeImpl(DhtUtil.getNumberFromUser(DhtUtil.sizeMessage, DhtUtil.sizeErrorMessage));
		} catch (Exception e) {
			e.printStackTrace();
			throw e;
		}

		TTransportFactory factory = new TFramedTransport.Factory();
		DhtSuperNode.Processor<DhtSuperNodeImpl> processor = new DhtSuperNode.Processor<>(handler);
		TThreadPoolServer.Args arguments = new TThreadPoolServer.Args(serverTransport);
		arguments.processor(processor);
		arguments.transportFactory(factory);
		TServer server = new TThreadPoolServer(arguments);
		server.serve();
	}
}
