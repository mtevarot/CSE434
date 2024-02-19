// Implements the client side of an echo client-server application program.
// The client reads ITERATIONS strings from stdin, passes the string to the
// server, which simply echoes it back to the client.
//
// Compile on general.asu.edu as:
//   g++ -o client UDPEchoClient.c
//
// Only on general3 and general4 have the ports >= 1024 been opened for
// application programs.
#include <iostream>
#include <sys/socket.h>   // For socket() and bind()
#include <arpa/inet.h>    // For sockaddr_in and inet_ntoa()
#include <cstdlib>        // For atoi() and std::exit()
#include <cstring>        // For memset()
#include <unistd.h>       // For close()
#include <sstream>        // parsing strings
#include <fstream> 
#include <vector>
#include <unordered_map>
#include <string>

#define ECHOMAX 255     // Longest string to echo
#define ITERATIONS	5   // Number of iterations the client executes

bool fileParsed = false;

void DieWithError( const char *errorMessage ) // External error handling function
{
    perror( errorMessage );
    exit(1);
}

struct StormEvent {
    int eventID;
    std::string state;
    int year;
    std::string monthName;
    std::string eventType;
    char czType;
    std::string czName;
    int injuriesDirect;
    int injuriesIndirect;
    int deathsDirect;
    int deathsIndirect;
    std::string damageProperty;
    std::string damageCrops;
    std::string torFScale;
};

std::vector<StormEvent> ParseCSV(const std::string& filename, std::vector<StormEvent> events, int& entries) {
    std::ifstream file(filename);
    std::string line;

    if (!file.is_open()) {
        std::cerr << "Unable to open file: " << filename << std::endl;
    }

    // Skip the header line
    std::getline(file, line);

    while (std::getline(file, line)) {
        std::istringstream s(line);
        StormEvent event;
        std::string field;

        std::getline(s, field, ',');
        event.eventID = std::stoi(field);
        std::getline(s, event.state, ',');
        std::getline(s, field, ',');
        event.year = std::stoi(field);
        std::getline(s, event.monthName, ',');
        std::getline(s, event.eventType, ',');
        std::getline(s, field, ',');
        event.czType = field[0];
        std::getline(s, event.czName, ',');
        std::getline(s, field, ',');
        event.injuriesDirect = std::stoi(field);
        std::getline(s, field, ',');
        event.injuriesIndirect = std::stoi(field);
        std::getline(s, field, ',');
        event.deathsDirect = std::stoi(field);
        std::getline(s, field, ',');
        event.deathsIndirect = std::stoi(field);
        std::getline(s, event.damageProperty, ',');
        std::getline(s, event.damageCrops, ',');
        std::getline(s, event.torFScale, ',');

        events.push_back(event);
        entries++;
        fileParsed = true;
    }

    file.close();
    return events;
}

int main( int argc, char *argv[] )
{
    size_t nread;
    int sock;                        // Socket descriptor
    struct sockaddr_in echoServAddr; // Echo server address
    struct sockaddr_in fromAddr;     // Source address of echo
    unsigned short echoServPort;     // Echo server port
    unsigned int fromSize;           // In-out of address size for recvfrom()
    char *servIP;                    // IP address of server
    char *echoString = NULL;         // String to send to echo server
    size_t echoStringLen = ECHOMAX;               // Length of string to echo
    int respStringLen;               // Length of received response
    char* command; 
    char* peerName; 
    char *ipv4; 
    unsigned short mPort, pPort; 
    std::vector<StormEvent> events;
    int entries = 0; 
    using namespace std; 


    echoString = (char *) malloc( ECHOMAX );

    if (argc < 3)    // Test for correct number of arguments
    {
        std::cerr << "Usage: " << argv[0] << " <Server IP address> <Echo Port>\n";
        std::exit(1);
    }

    servIP = argv[ 1 ];  // First arg: server IP address (dotted decimal)
    echoServPort = atoi( argv[2] );  // Second arg: Use given port

    std::cout << "Client: Arguments passed: server IP " << servIP << ", port " << echoServPort << std::endl;

    // Create a datagram/UDP socket
    if( ( sock = socket( PF_INET, SOCK_DGRAM, IPPROTO_UDP ) ) < 0 )
        DieWithError( "client: socket() failed" );

    // Construct the server address structure
    memset( &echoServAddr, 0, sizeof( echoServAddr ) ); // Zero out structure
    echoServAddr.sin_family = AF_INET;                  // Use internet addr family
    echoServAddr.sin_addr.s_addr = inet_addr( servIP ); // Set server's IP address
    echoServAddr.sin_port = htons( echoServPort );      // Set server's port



	//std::cout << "Client: Echoing strings for " << ITERATIONS << " iterations\n";

    for (int i = 0; i < ITERATIONS; ++i) {
        std::cout << "\nEnter command: \n";
        std::string echoString;
        std::getline(std::cin, echoString); // Read input
        string lastCommand; 


        // Send the string to the server
        if (sendto(sock, echoString.c_str(), echoString.length(), 0, (struct sockaddr*)&echoServAddr, sizeof(echoServAddr)) != echoString.length()) {
            DieWithError("sendto() sent a different number of bytes than expected");
        }

        lastCommand = echoString;

        // Receive a response
        unsigned int fromSize = sizeof(echoServAddr);
        char buffer[ECHOMAX + 1]; // +1 to null-terminate
        int respStringLen;
        if ((respStringLen = recvfrom(sock, buffer, ECHOMAX, 0, (struct sockaddr*)&echoServAddr, &fromSize)) > ECHOMAX) {
            DieWithError("recvfrom() failed");
        }

        buffer[respStringLen] = '\0'; // Null-terminate the received data

        string response(buffer); 

        std::cout << buffer << " \n";

        if(lastCommand.find("setup-dht") == 0 && response.substr(0, 7) == "SUCCESS") {
            
            string filename = "details-1950.csv";
            events = ParseCSV(filename, events, entries);

            std::unordered_map<int, std::string> peerNames;
            istringstream commandStream(lastCommand);
            string command, leaderName; 
            int sizeOfDHT, year; 

            commandStream >> command >> leaderName >> sizeOfDHT >> year;
            
            istringstream iss(response.substr(8)); // Start parsing right after "SUCCESS"
            string line;
            int ID = 0;
        
            while(getline(iss, line)) {
                string name, ip, temp; 
                unsigned short port; 

                if (line.empty() || line.find('{') == std::string::npos) continue;

                size_t infoStart = line.find('{') + 1; // Position after '{'
                size_t infoEnd = line.rfind('}'); // Position of '}'
                string peerInfo = line.substr(infoStart, infoEnd - infoStart);

                std::istringstream peerStream(peerInfo);


                getline(peerStream, name, ',');
                getline(peerStream, ip, ',');
                peerStream >> port;

                name.erase(0, name.find_first_not_of(" ")); 
                ip.erase(0, ip.find_first_not_of(" "));
                
                peerNames[ID] = name;
                ID++;
            }

            
            
            std::vector<int> entriesPerPeer(sizeOfDHT, 0); // Initialize a vector to keep track of entries per peer

            
            for (int i = 0; i < entries; ++i) {
                
                int peerIndex = i % sizeOfDHT;
                entriesPerPeer[peerIndex]++;
            }

            
            for (int i = 0; i < sizeOfDHT; ++i) {
                std::cout << peerNames[i] << " has " << entriesPerPeer[i] << " entries." << std::endl;
            }
            
            cout << "\nSUCCESS: dht-complete" << endl; 
        }

        if(echoString == "close") {
            break; 
        }
    }
    
    close( sock );
    exit( 0 );
}
