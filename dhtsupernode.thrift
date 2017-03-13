exception InvalidAddress {
  1: i32 errorCode,
  2: string message
}

exception InvalidNode {
  1: i32 errorCode,
  2: string message
}

service DhtSuperNode {
	string join(1: string ip, 2: i32 port) throws (1:InvalidAddress excpt1, 2:InvalidNode excpt2),
	void postJoin(1: string ip, 2: i32 port) throws (1:InvalidNode excpt),
    string getNodeList(),
	string getNode(),
	i32 getMaxSize(),
}
