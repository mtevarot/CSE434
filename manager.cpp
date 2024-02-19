// Implements the server side of an echo client-server application program.
// The client reads ITERATIONS strings from stdin, passes the string to the
// this server, which simply sends the string back to the client.
//
// Compile on general.asu.edu as:
//   g++ -o server UDPEchoServer.c
//
// Only on general3 and general4 have the ports >= 1024 been opened for
// application programs.
#include <iostream>       // For std::cout, std::cerr
#include <sys/socket.h>   // For socket() and bind()
#include <arpa/inet.h>    // For sockaddr_in and inet_ntoa()
#include <cstdlib>        // For atoi() and exit()
#include <cstring>        // For memset()
#include <string>         // includes string
#include <unordered_map>  // includes hashmap 
#include <sstream>        // parsing strings
#include <unistd.h>       // For close()
using namespace std; 

#define ECHOMAX 255     // Longest string to echo
#define MAX_PEERS 10    //max peers in hash table

int dhtCreated = 0; 

class PeerInfo {
public:
    //char name[15];
    string ipv4;
    unsigned short mPort;
    unsigned short pPort;
    string state;
};

unordered_map<string, PeerInfo> peers; 

void DieWithError( const char *errorMessage ) // External error handling function
{
    perror( errorMessage );
    std::exit( 1 );
}

bool registerPeer(const string& command) {
    istringstream iss(command);
    string action, name, ipv4, state; 
    unsigned short mPort, pPort; 

    iss >> action >> name >> ipv4 >> mPort >> pPort; 

    if(action == "register" && iss) {
        PeerInfo info = {ipv4, mPort, pPort, "Free"};
        auto result = peers.insert({name, info}); 
        if(result.second) {
            cout << "SUCCESS\n";      
            return true;
        } else {
            cout << "FAILURE\n";
            return false; 
        }
    } 

    return false; 
}

bool setup_DHT(const string& command, string& sResponse) {
    istringstream iss(command); 
    string action, leaderName;
    int sizeOfDHT, yearOfData; 

    iss >> action >> leaderName >> sizeOfDHT >> yearOfData; 

    if(action == "setup-dht" && iss) {
        int freePeers = 0;
        for (const auto& peer : peers) {
            if (peer.second.state == "Free") {
                ++freePeers;
            }
        }

        if(freePeers >= sizeOfDHT && freePeers >= 3 && dhtCreated == 0) {
            cout << "SUCCESS\n";

            auto leader = peers.find(leaderName); 
            if(leader != peers.end() && leader->second.state == "Free") {

                leader->second.state = "Leader";
                cout << "Leader: {" << leaderName << ", " <<  leader->second.ipv4 << ", " << leader->second.pPort << "}" << endl;
                sResponse += "{" + leaderName + ", " +  leader->second.ipv4 + ", " + to_string(leader->second.pPort) + to_string(yearOfData) + "}\n";
            } else {
                cout << "FAILURE" << endl;
                return false; 
            }


            int count = 0;
            for(auto& peer : peers) {
                if(peer.second.state == "Free" && count < sizeOfDHT - 1) {
                    peer.second.state = "inDHT";
                    cout << "Peer: {" << peer.first << ", " <<  peer.second.ipv4 << ", " << peer.second.pPort << "}" <<  endl;
                    sResponse += "{" + peer.first + ", " +  peer.second.ipv4 + ", " + to_string(peer.second.pPort) + to_string(yearOfData) + "}\n"; 
                    count++; 
                }
            }
            
            
            return true;
        } else {
            cout << "FAILURE" << endl;
        }
    } 

    return false; 
}




int main( int argc, char *argv[] )
{
    int sock;                        // Socket
    struct sockaddr_in echoServAddr; // Local address of server
    struct sockaddr_in echoClntAddr; // Client address
    unsigned int cliAddrLen;         // Length of incoming message
    char echoBuffer[ ECHOMAX ];      // Buffer for echo string
    unsigned short echoServPort;     // Server port
    int recvMsgSize;                 // Size of received message

    if( argc != 2 )         // Test for correct number of parameters
    {
        std::cerr << "Usage:  " << argv[0] << " <UDP SERVER PORT>\n";
        exit( 1 );
    }

    echoServPort = std::atoi(argv[1]);  // First arg: local port

    // Create socket for sending/receiving datagrams
    if( ( sock = socket( PF_INET, SOCK_DGRAM, IPPROTO_UDP ) ) < 0 )
        DieWithError( "server: socket() failed" );

    // Construct local address structure */
    memset( &echoServAddr, 0, sizeof( echoServAddr ) ); // Zero out structure
    echoServAddr.sin_family = AF_INET;                  // Internet address family
    echoServAddr.sin_addr.s_addr = htonl( INADDR_ANY ); // Any incoming interface
    echoServAddr.sin_port = htons( echoServPort );      // Local port

    // Bind to the local address
    if( bind( sock, (struct sockaddr *) &echoServAddr, sizeof(echoServAddr)) < 0 )
        DieWithError( "server: bind() failed" );

	std::cout << "Server: Port server is listening to is: " << echoServPort << std::endl;

    for(;;) // Run forever
    {
        cliAddrLen = sizeof( echoClntAddr );

        // Block until receive message from a client
        if( ( recvMsgSize = recvfrom( sock, echoBuffer, ECHOMAX, 0, (struct sockaddr *) &echoClntAddr, &cliAddrLen )) < 0 )
            DieWithError( "server: recvfrom() failed" );

        echoBuffer[ recvMsgSize ] = '\0';

        if(strcmp(echoBuffer, "close") == 0) {
            const char *response = "Shutting down"; 
            if( sendto( sock, response, strlen( response ), 0, (struct sockaddr *) &echoClntAddr, sizeof( echoClntAddr ) ) != strlen( response ) ) 
                DieWithError( "FAILURE\n" );

            break;
        }

        struct PeerInfo peer;
        string sResponse = ""; 
        string sucResponse = "";

        if(registerPeer(echoBuffer)) {
            const char *response = "SUCCESS\n"; 
            
            if( sendto( sock, response, strlen( response ), 0, (struct sockaddr *) &echoClntAddr, sizeof( echoClntAddr ) ) != strlen( response ) ) 
                DieWithError( "FAILURE\n" );
            
            
        } else if(setup_DHT(echoBuffer, sResponse)) {
            sucResponse += "SUCCESS\n";
            sucResponse += sResponse;
            const char *response = sucResponse.c_str();
            
            if( sendto( sock, response, strlen( response ), 0, (struct sockaddr *) &echoClntAddr, sizeof( echoClntAddr ) ) != strlen( response ) ) 
                DieWithError( "FAILURE\n" ); 
        } else {
            const char *response = "FAILURE\n"; 

            if( sendto( sock, response, strlen( response ), 0, (struct sockaddr *) &echoClntAddr, sizeof( echoClntAddr ) ) != strlen( response ) )
            DieWithError( "FAILURE" );
        }
            
            /*
            printf("Registered peer - Name: %s, IPv4: %s, mPort: %d, pPort: %d, State: %s\n",
                   peer.name, peer.ipv4, peer.mPort, peer.pPort, peer.state);
                   */


        //printf( "we registered this ``%s'' and sent it here %s\n", echoBuffer, inet_ntoa( echoClntAddr.sin_addr ) );

        // Send received datagram back to the client
        /*
        if( sendto( sock, echoBuffer, strlen( echoBuffer ), 0, (struct sockaddr *) &echoClntAddr, sizeof( echoClntAddr ) ) != strlen( echoBuffer ) )
            DieWithError( "server: sendto() sent a different number of bytes than expected" );
            */
    }
    close(sock);
    exit(0);
    return 0;
}
