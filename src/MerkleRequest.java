import java.nio.file.Path;

enum RequestType {ADD, DELETE, STOP;}

public class MerkleRequest {
    // MerkleRequest class provides formal formatting for the requests to push the queue in MerkleReplica
    public RequestType requestType;
    public Path src;

    MerkleRequest(RequestType requestType , Path src) {
        this.requestType = requestType;
        this.src = src;
    }
}
