#include <stdio.h>
#include <stdlib.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netdb.h>
#include <memory.h>
#include <errno.h>
#include <string.h>
#include <stdbool.h>
#include <limits.h>

// had to add these in for 'fd_set'...instructor didn't include them in his...dumb...
#include <sys/select.h>
#include <sys/types.h>
#include <unistd.h>
#include <arpa/inet.h>

// platform-specific includes
#ifdef _WIN32
    #include <winsock2.h>
    #include <ws2tcpip.h>
    #pragma comment(lib, "Ws2_32.lib") // link winsock library
#else
    #include <arpa/inet.h>  // for inet_pton on linux
#endif

#define MAX_RETRIES 3   // max retries for incorrect input for PUT/GET requests

typedef enum {
    PUT_FORWARD,
    GET_FORWARD,
    WHAT_X,
    GET_REPLY_X,
    PUT_REPLY_X
} MessageType;

typedef struct app_msg{
    MessageType msg_id;         // 4 bytes
    unsigned int key;           // 4 bytes
    unsigned int value;         // 4 bytes
    struct in_addr ip_addr;     // 4 bytes  (binary format for IPv4) (8 bytes for IPv6)
    unsigned int tcp_port;      // 4 bytes
} app_msg_t;

/* Hash Table */
#define TABLE_SIZE 10
#define NODE_COUNT 6

typedef struct HT_Node {
    unsigned int *key;
    unsigned int *value;
    struct HT_Node *next;
} HT_Node;

typedef struct HashTable {
    HT_Node *buckets[TABLE_SIZE];
} HashTable;

static unsigned int node_hash(unsigned int key){
    return ((key * 2654435761U) % NODE_COUNT) + 1;  // adding 1 adjusts the range to [1, NODE_COUNT]
}

static unsigned int table_hash(unsigned int key){
    return (key * 2654435761U) % TABLE_SIZE;
}

static HashTable *create_table(){
    HashTable *table = malloc(sizeof(HashTable));
    if (!table){
        perror("Failed to create hash table\n");
        exit(EXIT_FAILURE);
    }
    for (int i = 0; i < TABLE_SIZE; i++){
        table->buckets[i] = NULL;
    }
    return table;
}

static HT_Node *create_node(unsigned int key, unsigned int value){
    HT_Node *newNode = malloc(sizeof(HT_Node));

    if (!newNode){
        perror("Failed to create a new hash table node\n");
        exit(EXIT_FAILURE);
    }

    newNode->key = malloc(sizeof(unsigned int));
    newNode->value = malloc(sizeof(unsigned int));

    if (!newNode->key || !newNode->value){
        perror("Failed to allocate memory for key or value while creating a new HT_Node\n");
        free(newNode->key);
        free(newNode->value);
        free(newNode);
        exit(EXIT_FAILURE);
    }

    *(newNode->key) = key;
    *(newNode->value) = value;
    newNode->next = NULL;

    return newNode;
}

static void insert(HashTable *table, const unsigned int *key, const unsigned int *value){
    unsigned int index = table_hash((unsigned int)*key);

    // create the new node
    HT_Node *new_node = create_node((unsigned int)*key, (unsigned int)*value);

    // if the current bucket doesn't hold any key-value pair
    if (table->buckets[index] == NULL){
        table->buckets[index] = new_node;
    } else { // if the current bucket is already holding a key-value pair (collision handled using linked list)
        HT_Node *current = table->buckets[index];
        while (current->next != NULL){
            current = current->next;
        }
        current->next = new_node;
    }
}

static unsigned int search(HashTable *table, const unsigned int *key){
    unsigned int index = table_hash((unsigned int)*key);

    HT_Node *current = table->buckets[index];

    while (current != NULL){
        if (*(current->key) == *key){
            return *current->value;
        }
        current = current->next;
    }
    return UINT_MAX;    // key not found retrun max value for uint as sentinel value
}

/* UNUSED */
// static void delete(HashTable *table, const unsigned int *key){
//     unsigned int index = table_hash((unsigned int)*key);

//     HT_Node *current = table->buckets[index];
//     HT_Node *prev = NULL;

//     while (current != NULL && *(current->key) != *key){ // search till NULL or key is found
//         prev = current;
//         current = current->next;
//     }

//     if (current == NULL){ // didn't find it
//         printf("Key not found : %i\n", (unsigned int)*key);
//         return;
//     }

//     if (prev == NULL){  // found it (is it the head?)
//         table->buckets[index] = current->next;
//     } else {    // not the head
//         prev->next = current->next;
//     }

//     free(current->key);
//     free(current->value);
//     free(current);
//     printf("Key deleted: %i`n", (unsigned int)*key);

//     return;
// }
/* End Hash Table */


#define BUFFER_SIZE 24    // 24 bytes maximum for app_msg_t (adding 4 bytes for forward compatibility with IPv6)
#define INPUT_BUFFER_SIZE 32    // 27 bytes maximum expected for PUT request (round up to 32)

// helper function to initialize a new app_msg_t
app_msg_t *init_app_msg(MessageType msg_id){
    app_msg_t* msg = calloc(1, sizeof(app_msg_t));
    if (!msg) {
        perror("Failed to allocate memory for app_msg_t");
        exit(EXIT_FAILURE);
    }
    msg->msg_id = msg_id;
    return msg;
}

app_msg_t* create_put_forward(unsigned int key, struct in_addr ip_addr, unsigned int tcp_port) {
    app_msg_t* msg = init_app_msg(PUT_FORWARD);
    msg->key = key;
    msg->ip_addr = ip_addr;
    msg->tcp_port = tcp_port;
    return msg;
}

app_msg_t* create_get_forward(unsigned int key, struct in_addr ip_addr, unsigned int tcp_port) {
    app_msg_t* msg = init_app_msg(GET_FORWARD);
    msg->key = key;
    msg->ip_addr = ip_addr;
    msg->tcp_port = tcp_port;
    return msg;
}

app_msg_t* create_put_reply_x(unsigned int key, unsigned int value) {
    app_msg_t* msg = init_app_msg(PUT_REPLY_X);
    msg->key = key;
    msg->value = value;
    return msg;
}

app_msg_t* create_get_reply_x(unsigned int key, unsigned int value) {
    app_msg_t* msg = init_app_msg(GET_REPLY_X);
    msg->key = key;
    msg->value = value;
    return msg;
}

app_msg_t* create_what_x(unsigned int key) {
    app_msg_t* msg = init_app_msg(WHAT_X); // Initialize the message
    msg->key = key; // Set only the key, as nothing else is needed
    return msg;
}


// helper function to get valid unsgined integer from stdin
static unsigned int get_valid_unsigned_int(const char* prompt){
    char input[INPUT_BUFFER_SIZE];
    char* endptr;
    int retries = 0;

    while(retries < MAX_RETRIES){
        printf("%s", prompt);
        if (fgets(input, sizeof(input), stdin) == NULL){
            fprintf(stderr, "Error reading input. Please try again.\n");
            retries++;
            continue;
        }

        // remove newline character if present
        input[strcspn(input, "\n")] = '\0';     // substitute with null

        // convert input to integer
        unsigned long value = strtoul(input, &endptr, 10);

        // check if the input is a valid unsigned integer
        if (*endptr == '\0' && value <= UINT_MAX && value > 0){
            return (unsigned int)value;
        }

        fprintf(stderr, "Invalid input. Please enter a number > 0.\n");
        retries++;
    }

    return UINT_MAX;    // return sentinal value if we reach max number of retries
}



size_t serialize_app_msg(const app_msg_t* message, char* buffer, size_t buffer_size){

    // check buffer size
    if (buffer_size < sizeof(app_msg_t)){
        fprintf(stderr, "Buffer to small for serialization\n");
        return 0;
    }

    size_t offset = 0;

    // serialize fields into the buffer and convert from host byte order to network byte order
    unsigned int msg_id_net = htonl(message->msg_id);
    memcpy(buffer + offset, &msg_id_net, sizeof(msg_id_net));                               // msg_id
    offset += sizeof(msg_id_net);

    unsigned int key_net = htonl(message->key);
    memcpy(buffer + offset, &key_net, sizeof(key_net));                                     // key
    offset += sizeof(key_net);

    unsigned int value_net = htonl(message->value);
    memcpy(buffer + offset, &value_net, sizeof(value_net));                                 // value
    offset += sizeof(value_net);

    unsigned int ip_net = htonl(message->ip_addr.s_addr);
    memcpy(buffer + offset, &ip_net, sizeof(ip_net));                                       // ip address
    offset += sizeof(ip_net);

    unsigned int tcp_net = htonl(message->tcp_port);
    memcpy(buffer + offset, &tcp_net, sizeof(tcp_net));                                     // tcp port
    offset += sizeof(tcp_net);

    return offset;      // return total number of bytes written.

}

app_msg_t *deserialize_app_msg(const char* buffer){

    // allocate memory for deserialization
    app_msg_t* message = calloc(1, sizeof(app_msg_t));
    if (!message){
        perror("Failed to allocate memory for deserialized app message.\n");
        exit(EXIT_FAILURE);
    }

    size_t offset = 0;

    // deserialize fields from the buffer and convert from network byte order to host byte order
    unsigned int msg_id_net;
    memcpy(&msg_id_net, buffer + offset, sizeof(msg_id_net));                               // msg_id
    message->msg_id = ntohl(msg_id_net);
    offset += sizeof(msg_id_net);

    unsigned int key_net;
    memcpy(&key_net, buffer + offset, sizeof(key_net));                                     // key
    message->key = ntohl(key_net);
    offset += sizeof(key_net);

    unsigned int value_net;
    memcpy(&value_net, buffer + offset, sizeof(value_net));                                 // value
    message->value = ntohl(value_net);
    offset += sizeof(value_net);

    unsigned int ip_net;
    memcpy(&ip_net, buffer + offset, sizeof(ip_net));                                       // ip address
    message->ip_addr.s_addr = ntohl(ip_net);
    offset += sizeof (ip_net);

    unsigned int tcp_net;
    memcpy(&tcp_net, buffer + offset, sizeof(tcp_net));                                     // tcp port
    message->tcp_port = ntohl(tcp_net);
    offset += sizeof(tcp_net);

    return message;
}







static int is_valid_ip(const char *ip){
    #ifdef _WIN32
        WSADATA wsaData;
        if (WSAStartup(MAKEWORD(2, 2), &wsaData) != 0){
            printf("WSAStartup failed\n");
            return -1;
        } else {
            return 1;
        }
        #if _WIN32_WINNT >= 0x0600 // Vista or later
            struct sockaddr_in6 sa;
            int result = inet_pton(AF_INET6, ip, &(sa.sin6_addr));
            WSACleanup();
            return result;
        #else
            // fallback for older windows systems
            unsigned long addr = inet_addr(ip_str);
            WSACleanup();
            if (addr == INADDR_NONE){
                return 0; // invalid
            }
            return 1; // valid
        #endif
    #else // linux/unix
        struct sockaddr_in sa;
        int result = inet_pton(AF_INET, ip, &(sa.sin_addr));
        return result;
    #endif
}

/* UNUSED */
// static unsigned long ip_to_int(const char *ip_str){
//     struct sockaddr_in sa;

//     #ifdef _WIN32
//         WSADATA wsaData;
//         if (WSAStartup(MAKEWORD(2, 2), &wsaData) != 0){
//             printf("WSAStartup failed\n");
//             return 0;
//         }
//     #endif

//     // validate and conver the ip to binary format
//     if (inet_pton(AF_INET, ip_str, &(sa.sin_addr)) != 1){
//         #ifdef _WIN32
//             WSACleanup();
//         #endif
//         return 0;
//     }

//     #ifdef _WIN32
//         WSACleanup();
//     #endif

//     // conver the binary ip to an integer
//     return ntohl(sa.sin_addr.s_addr);   // convert to host byte order (inet_pton returns in network byte order)
// }









static void manage_node_communication(      const char *ip, 
                                            const unsigned int udp_port, 
                                            const unsigned int successor_udp, 
                                            const unsigned int tcp_port, 
                                            const unsigned int node_id,
                                            HashTable *hash_table){

    // create a UDP socket
    int udp_sock_fd = socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP);
    if (udp_sock_fd < 0){
        printf("Error creating UDP socket\n");
        exit(1);
    }

    // create a TCP socket
    int tcp_sock_fd = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    if (tcp_sock_fd < 0){
        printf("Error creating TCP socket\n");
        exit(1);
    }
    // allow TCP master socket multiple connections
    int opt = 1;
    if (setsockopt(tcp_sock_fd, SOL_SOCKET, SO_REUSEADDR, (char *)&opt, sizeof(opt)) < 0){
        printf("TCP socket creation failed for multiple connectinos\n");
        exit(EXIT_FAILURE);
    }
    
    // file descriptor set
    fd_set readyfds, masterfds;

    // server and client credentials for tcp and udp
    struct sockaddr_in tcp_server_addr, tcp_client_addr, udp_server_addr;

    // specify server info
    tcp_server_addr.sin_family = AF_INET;
    udp_server_addr.sin_family = AF_INET;
    tcp_server_addr.sin_port = htons(tcp_port);
    udp_server_addr.sin_port = htons(udp_port);

    // convert IP address from string to binary format and store in server info
    if (inet_pton(AF_INET, ip, &tcp_server_addr.sin_addr) <= 0){
        perror("Invalid IP address for TCP server\n");
        exit(EXIT_FAILURE);
    }
    if (inet_pton(AF_INET, ip, &udp_server_addr.sin_addr) <= 0){
        perror("Invalid IP address for UDP server\n");
        exit(EXIT_FAILURE);
    }

    // Bind the server to the udp socket
    if (bind(udp_sock_fd, (struct sockaddr *)&udp_server_addr, sizeof(struct sockaddr)) == -1){
        printf("UDP Socket Bind Failed...\n");
        return;
    }

    // Bind the server to the tcp socket
    if (bind(tcp_sock_fd, (struct sockaddr *)&tcp_server_addr, sizeof(struct sockaddr)) == -1){
        printf("TCP Socket Bind Failed...\n");
        return;
    }

    // Listen on TCP socket (UDP is connectionless so no need to listen)
    if (listen(tcp_sock_fd, 5) < 0){
        printf("TCP Listen Failed...\n");
        return;
    }

    // Initialize the FD sets
    FD_ZERO(&readyfds);
    FD_ZERO(&masterfds);

    // Add UDP, TCP master, and console FDs to the master set of FDs
    FD_SET(udp_sock_fd, &masterfds);
    FD_SET(tcp_sock_fd, &masterfds);
    FD_SET(0, &masterfds);

    int max_fd = udp_sock_fd > tcp_sock_fd ? udp_sock_fd : tcp_sock_fd;

    unsigned int key = 0, value = 0;

    while(1){

        readyfds = masterfds;   // copy master set of FDs (select will remove any that aren't "ready") into set of ready FDs for select to filter through.

        printf("Waiting for input (PUT/GET) or TCP/UDP socket to ready...\n");

        select(max_fd + 1, &readyfds, NULL, NULL, NULL);

        /*****************************************************************/
        /*                      TCP NEW CONNECTION                       */
        /*****************************************************************/

        if (FD_ISSET(tcp_sock_fd, &readyfds)){   // connection request received from client

            struct sockaddr_in new_tcp_client_addr;
            socklen_t addr_len = sizeof(new_tcp_client_addr);

            int new_sock_fd = accept(tcp_sock_fd, (struct sockaddr *)&new_tcp_client_addr, &addr_len);

            if (new_sock_fd < 0){
                printf("Accept Error: %d\n", errno);
                exit(0);
            }

            if (new_sock_fd == 0){
                fprintf(stderr, "I think this is an error. FD = 0 is reserved for stdin so the new socket fd should not be 0...");
                exit(0);
            }

            printf("Connection accepted from client: %s:%u\n", inet_ntoa(tcp_client_addr.sin_addr), ntohs(tcp_client_addr.sin_port));

            // add the new connection socket to the active set
            FD_SET(new_sock_fd, &masterfds);
            if (new_sock_fd > max_fd){
                max_fd = new_sock_fd;  // update the max file descriptor
            }
        } 

        /*****************************************************************/
        /*                    TCP/UDP COMMUNICATION                      */
        /*****************************************************************/

        // loop through all FDs
        for (int fd = 1; fd <= max_fd; fd++){
            
            /*** UDP COMMUNICATION ***/

            if (fd == udp_sock_fd && FD_ISSET(fd, &readyfds)){  

                char buffer[BUFFER_SIZE];
                struct sockaddr_in sender_addr;
                socklen_t addr_len = sizeof(sender_addr);

                ssize_t received_len = recvfrom(udp_sock_fd, buffer, BUFFER_SIZE, 0, (struct sockaddr *)&sender_addr, &addr_len);

                if (received_len < 0){
                    perror("Error receiving data\n");
                } else if (received_len > BUFFER_SIZE){
                    printf("Invalid message size: %zd bytes (expected %i bytes)\n", received_len, BUFFER_SIZE);
                } else {

                    // deserialize the buffer into the app_msg_t structure
                    app_msg_t *recv_msg = deserialize_app_msg(buffer);
                    
                    // Print sender details
                    printf("Message received from %s:%d\n", inet_ntoa(sender_addr.sin_addr), ntohs(sender_addr.sin_port));

                    // print to console
                    printf("Received Message:\n");
                    printf("  Msg ID:    %u\n", recv_msg->msg_id);
                    printf("  Key:       %u\n", recv_msg->key);
                    printf("  Value:     %u\n", recv_msg->value);
                    printf("  IP Addr:   %s\n", inet_ntoa(*(struct in_addr *)&recv_msg->ip_addr));
                    printf("  TCP Port:  %u\n", recv_msg->tcp_port);

                    // process message
                    if (recv_msg->msg_id == PUT_FORWARD){   // PUT_FORWARD msg received from another node

                        if (node_hash(recv_msg->key) == node_id){    // key is destined for this node

                            /**
                             * Need to send WHAT_X over TCP to the originator to get the value
                             * Must establish TCP connection with originator
                             */

                            // Initialize variables and specify server credentials
                            struct sockaddr_in dest;
                            memset(&dest, 0, sizeof(dest));
                            dest.sin_family = AF_INET;
                            dest.sin_port = htons(recv_msg->tcp_port);

                            // Resolve the hostname to an IP address
                            char ip_str[INET_ADDRSTRLEN];
                            inet_ntop(AF_INET, &recv_msg->ip_addr, ip_str, sizeof(ip_str));  // convert IP to string and store in ip_str
                            struct hostent *host = gethostbyname(ip_str);
                            if (host == NULL) {
                                fprintf(stderr, "Error resolving hostname\n");
                                exit(EXIT_FAILURE);
                            }
                            dest.sin_addr = *((struct in_addr *)host->h_addr_list[0]);

                            // Create the communication socket
                            int sockfd = socket(AF_INET, SOCK_STREAM, 0);
                            if (sockfd < 0) {
                                perror("Socket creation failed");
                                exit(EXIT_FAILURE);
                            }

                            // Connect to the server
                            if (connect(sockfd, (struct sockaddr *)&dest, sizeof(dest)) < 0) {
                                perror("Connection failed");
                                close(sockfd);
                                FD_CLR(sockfd, &masterfds); // clear from fd set
                                exit(EXIT_FAILURE);
                            }

                            printf("Connection established with server: %s:%u\n",
                                inet_ntoa(dest.sin_addr), ntohs(dest.sin_port));

                            // Add the new connection socket to the active FD set
                            FD_SET(sockfd, &masterfds);
                            if (sockfd > max_fd) {
                                max_fd = sockfd;  // Update the max file descriptor
                            }

                            // prepare data WHAT_X
                            app_msg_t *what_x = create_what_x(recv_msg->key);
                            size_t serialized_size = serialize_app_msg(what_x, buffer, BUFFER_SIZE);
                            if (serialized_size > BUFFER_SIZE){
                                fprintf(stderr, "Serialized response for WHAT_X exceeds buffer size\n");
                                exit(EXIT_FAILURE);
                            }

                            // send data
                            int sent_recv_bytes = send(sockfd, buffer, serialized_size, 0);

                            if (sent_recv_bytes < 0){
                                perror("WHAT_X send failed.\n");
                            } else {
                                printf("WHAT_X sent to %s:%u. %zu bytes sent...\n", ip_str, recv_msg->tcp_port, sent_recv_bytes);
                            }

                            free(what_x);   // free allocated memory

                        } else {    // PUT_FORWARD to successor node over UDP

                            // set up receiver address
                            struct sockaddr_in receiver_addr;
                            memset(&receiver_addr, 0, sizeof(receiver_addr));
                            receiver_addr.sin_family = AF_INET;
                            receiver_addr.sin_port = htons(successor_udp);
                            receiver_addr.sin_addr.s_addr = inet_addr(ip);

                            // set up message
                            app_msg_t *put_fwd = create_put_forward(recv_msg->key, recv_msg->ip_addr, recv_msg->tcp_port);
                            size_t serialized_size = serialize_app_msg(put_fwd, buffer, BUFFER_SIZE);
                            if (serialized_size > BUFFER_SIZE){
                                fprintf(stderr, "Serialized response for PUT_FORWARD exceeds buffer size\n");
                                exit(EXIT_FAILURE);
                            }

                            // send message
                            int sent_bytes = sendto(udp_sock_fd, buffer, serialized_size, 0, (struct sockaddr *)&receiver_addr, sizeof(receiver_addr));

                            if (sent_bytes < 0){
                                perror("PUT_FORWARD send failed\n");
                            } else {
                                printf("Message forwarded to %s:%u\n", ip, successor_udp);
                            }

                            free(put_fwd);  // free allocated memory
                        }

                    } else if (recv_msg->msg_id == GET_FORWARD){    // GET_FORWARD msg received from another node
                        
                        if (node_hash(recv_msg->key) == node_id){
                            
                            // send GET_REPLY_X to originator over TCP

                            // Initialize variables and specify server credentials
                            struct sockaddr_in dest;
                            memset(&dest, 0, sizeof(dest));
                            dest.sin_family = AF_INET;
                            dest.sin_port = htons(recv_msg->tcp_port);

                            // Resolve the hostname to an IP address
                            char ip_str[INET_ADDRSTRLEN];
                            inet_ntop(AF_INET, &recv_msg->ip_addr, ip_str, sizeof(ip_str));  // convert IP to string and store in ip_str
                            struct hostent *host = gethostbyname(ip_str);
                            if (host == NULL) {
                                fprintf(stderr, "Error resolving hostname\n");
                                exit(EXIT_FAILURE);
                            }
                            dest.sin_addr = *((struct in_addr *)host->h_addr_list[0]);

                            // Create the communication socket
                            int sockfd = socket(AF_INET, SOCK_STREAM, 0);
                            if (sockfd < 0) {
                                perror("Socket creation failed");
                                exit(EXIT_FAILURE);
                            }

                            // Connect to the server
                            if (connect(sockfd, (struct sockaddr *)&dest, sizeof(dest)) < 0) {
                                perror("Connection failed");
                                close(sockfd);
                                FD_CLR(sockfd, &masterfds); // clear from fd set
                                exit(EXIT_FAILURE);
                            }

                            printf("Connection established with server: %s:%u\n",
                                inet_ntoa(dest.sin_addr), ntohs(dest.sin_port));

                            /*NOTE: No need to add 'sockfd' to the 'masterfds' because only this one GET_REPLY_X message needs to be sent.*/

                            // prepare data
                            app_msg_t *get_reply_x = create_get_reply_x(recv_msg->key, search(hash_table, &recv_msg->key));
                            size_t serialized_size = serialize_app_msg(get_reply_x, buffer, BUFFER_SIZE);
                            if (serialized_size > BUFFER_SIZE){
                                fprintf(stderr, "Serialized response for GET_REPLY_X exceeds buffer size\n");
                                exit(EXIT_FAILURE);
                            }

                            // send data
                            int sent_recv_bytes = send(sockfd, buffer, serialized_size, 0);

                            if (sent_recv_bytes < 0){
                                perror("GET_REPLY_X send failed.\n");
                            } else {
                                printf("GET_REPLY_X sent to %s:%u. %zu bytes sent...\n", ip_str, recv_msg->tcp_port, sent_recv_bytes);
                            }

                            // no need to continue communication after GET_REPLY_X
                            printf("Closing connection with %s:%u\n", inet_ntoa(dest.sin_addr), ntohs(dest.sin_port));
                            close(sockfd);
                            FD_CLR(sockfd, &masterfds); // clear from fd set
                            
                            free(get_reply_x);  // free allocated memory

                        } else {    // GET_FORWARD to successor node over UDP

                            // set up receiver address
                            struct sockaddr_in receiver_addr;
                            memset(&receiver_addr, 0, sizeof(receiver_addr));
                            receiver_addr.sin_family = AF_INET;
                            receiver_addr.sin_port = htons(successor_udp);
                            receiver_addr.sin_addr.s_addr = inet_addr(ip);

                            // set up message
                            app_msg_t *get_fwd = create_get_forward(recv_msg->key, recv_msg->ip_addr, recv_msg->tcp_port);
                            size_t serialized_size = serialize_app_msg(get_fwd, buffer, BUFFER_SIZE);
                            if (serialized_size > BUFFER_SIZE){
                                fprintf(stderr, "Serialized response for GET_FORWARD exceeds buffer size\n");
                                exit(EXIT_FAILURE);
                            }

                            // send message
                            ssize_t sent_bytes = sendto(udp_sock_fd, buffer, serialized_size, 0, (struct sockaddr *)&receiver_addr, sizeof(receiver_addr));

                            if (sent_bytes < 0){
                                perror("GET_FORWARD send failed\n");
                            } else {
                                printf("Message forwarded to %s:%u\n", ip, successor_udp);
                            }

                            free(get_fwd);
                        }

                    } else {
                        perror("Invalid message type for UDP communication received...\n");
                    }

                    free(recv_msg); // free allocated memory
                }

            /*** TCP COMMUNICATION ***/

            } else if (fd != tcp_sock_fd && fd != udp_sock_fd && FD_ISSET(fd, &readyfds)){

                // prepare message buffer
                char buffer[BUFFER_SIZE];
                memset(buffer, 0, BUFFER_SIZE);

                // store client socket information
                struct sockaddr_in sender_addr;
                socklen_t addr_len = sizeof(sender_addr);

                // get message
                size_t received_len = recv(fd, buffer, BUFFER_SIZE, 0);
                if (getpeername(fd, (struct sockaddr *)&sender_addr, &addr_len) != 0){
                    fprintf(stderr, "Debug: getpeername() failed. Sender info was not resolved...\n");
                }

                // print info to console
                printf("Server received %zu bytes from client %s:%u via TCP\n", received_len, inet_ntoa(sender_addr.sin_addr), ntohs(sender_addr.sin_port));

                // deserialize the buffer into the app_msg_t structure
                app_msg_t *recv_msg = deserialize_app_msg(buffer);

                // print message to console
                printf("Received Message:\n");
                printf("  Msg ID:    %u\n", recv_msg->msg_id);
                printf("  Key:       %u\n", recv_msg->key);
                printf("  Value:     %u\n", recv_msg->value);
                printf("  IP Addr:   %s\n", inet_ntoa(*(struct in_addr *)&recv_msg->ip_addr));
                printf("  TCP Port:  %u\n", recv_msg->tcp_port);

                // if server receives emtpy message from client, server may close the conection and wait for fresh new connection from client.
                if (received_len == 0){
                    close(fd);
                    FD_CLR(fd, &masterfds); // clear from fd set
                    break;
                }

                // handle tcp related messages
                if (recv_msg->msg_id == WHAT_X){ // send back PUT_REPLY_X

                    // double check we have the key value pair
                    if (recv_msg->key == key){   // if we do
                        
                        // prepare data
                        app_msg_t *put_reply_x = create_put_reply_x(recv_msg->key, value);
                        size_t serialized_size = serialize_app_msg(put_reply_x, buffer, BUFFER_SIZE);
                        if (serialized_size > BUFFER_SIZE){
                            fprintf(stderr, "Serialized response for PUT_REPLY_X exceeds buffer size\n");
                            exit(EXIT_FAILURE);
                        }

                        // send data
                        int sent_recv_bytes = send(fd, buffer, serialized_size, 0);

                        if (sent_recv_bytes < 0){
                            perror("PUT_REPLY_X failed...\n");
                        } else {
                            printf("PUT_REPLY_X sent to %s:%u\n", inet_ntoa(sender_addr.sin_addr), ntohs(sender_addr.sin_port));
                        }

                        printf("Closing connection with %s:%u\n", inet_ntoa(sender_addr.sin_addr), ntohs(sender_addr.sin_port));

                        close(fd);  // no need to continue communication after sending PUT_REPLY_X
                        FD_CLR(fd, &masterfds);     // clear from fd set

                        free(put_reply_x);  // free allocated memory

                        key = 0, value = 0; // reset key and value
                    }

                } else if (recv_msg->msg_id == GET_REPLY_X){

                    printf("GET request succeeded for Key = %i. Value = %i\n", recv_msg->key, recv_msg->value);

                    printf("Closing connection with %s:%u\n", inet_ntoa(sender_addr.sin_addr), ntohs(sender_addr.sin_port));
                    close(fd);  // no need to continue communication after receiving GET_REPLY_X
                    FD_CLR(fd, &masterfds); // clear from fd set

                } else if (recv_msg->msg_id == PUT_REPLY_X){

                    // double check we are the correct node
                    if (node_hash(recv_msg->key) == node_id){
                        insert(hash_table, &recv_msg->key, &recv_msg->value);
                        printf("Inserted new key-value pair for Key = %i\n", recv_msg->key);
                    } else {
                        printf("Received PUT_REPLY_X for key = %i, but this is not the correct node for storage...\n", recv_msg->key);
                    }

                    printf("Closing connection with %s:%u\n", inet_ntoa(sender_addr.sin_addr), ntohs(sender_addr.sin_port));
                    close(fd);  // no need to continue communication after receiving PUT_REPLY_X
                    FD_CLR(fd, &masterfds); // clear from fd set

                } else {
                    perror("Invalid message type for TCP communication received...\n");
                    break;
                }

                free(recv_msg); // free allocated memory

            }

        }
        
        /*****************************************************************/
        /*                      PUT/GET via Console                      */
        /*****************************************************************/

        if (FD_ISSET(0, &readyfds)){  // data arrives from console (user input)
            
            char input[INPUT_BUFFER_SIZE];
            if (fgets(input, INPUT_BUFFER_SIZE, stdin) != NULL){

                // remove newline character if present
                input[strcspn(input, "\n")] = '\0';     // substitute with null

                if (strncmp(input, "PUT", 3) == 0){
                    
                    key = get_valid_unsigned_int("Enter key: ");
                    value = get_valid_unsigned_int("Enter value: ");

                    if (key == UINT_MAX || value == UINT_MAX){
                        fprintf("Maximum number of retries reached...follow the instructions explicitly. Terminating request. Try again...\n");
                        continue;
                    }

                    if (node_hash(key) == node_id){    // store key and value in own hash table
                    
                        insert(hash_table, &key, &value);
                    
                    } else { // send PUT_FORWARD

                        // set up receiver address
                        struct sockaddr_in receiver_addr;
                        memset(&receiver_addr, 0, sizeof(receiver_addr));
                        receiver_addr.sin_family = AF_INET;
                        receiver_addr.sin_port = htons(successor_udp);
                        receiver_addr.sin_addr.s_addr = inet_addr(ip);

                        // set up message
                        char buffer[BUFFER_SIZE];
                        struct in_addr ip_addr;
                        inet_pton(AF_INET, ip, &ip_addr);
                        app_msg_t *put_fwd = create_put_forward(key, ip_addr, tcp_port);
                        size_t serialized_size = serialize_app_msg(put_fwd, buffer, BUFFER_SIZE);
                        if (serialized_size > BUFFER_SIZE){
                            fprintf(stderr, "Serialized response for PUT_FORWARD exceeds buffer size\n");
                            exit(EXIT_FAILURE);
                        }

                        // send message
                        ssize_t sent_bytes = sendto(udp_sock_fd, buffer, serialized_size, 0, (struct sockaddr *)&receiver_addr, sizeof(receiver_addr));

                        if (sent_bytes < 0){
                            perror("PUT_FORWARD send failed\n");
                        } else {
                            printf("Message forwarded to %s:%u\n", ip, successor_udp);
                        }

                        free(put_fwd);
                    }

                } else if (strncmp(input, "GET", 3) == 0){
                    
                    key = get_valid_unsigned_int("Enter key: ");

                    if (key == UINT_MAX){
                        fprintf("Maximum number of retries reached...follow the instructions explicitly. Terminating request. Try again...\n");
                        continue;
                    }

                    if (node_hash(key) == node_id){ // if this node is holding the key specified in the GET request from console

                        printf("Value for key = %i found in memory. Value = %i", key, search(hash_table, &key));

                    } else { // send GET_FORWARD

                        // set up receiver address
                        struct sockaddr_in receiver_addr;
                        memset(&receiver_addr, 0, sizeof(receiver_addr));
                        receiver_addr.sin_family = AF_INET;
                        receiver_addr.sin_port = htons(successor_udp);
                        receiver_addr.sin_addr.s_addr = inet_addr(ip);

                        // set up message
                        char buffer[BUFFER_SIZE];
                        struct in_addr ip_addr;

                        inet_pton(AF_INET, ip, &ip_addr);
                        app_msg_t *get_fwd = create_get_forward(key, ip_addr, tcp_port);
                        size_t serialized_size = serialize_app_msg(get_fwd, buffer, BUFFER_SIZE);

                        // send message
                        ssize_t sent_bytes = sendto(udp_sock_fd, buffer, serialized_size, 0, (struct sockaddr *)&receiver_addr, sizeof(receiver_addr));

                        if (sent_bytes < 0){
                            perror("GET_FORWARD send failed\n");
                        } else {
                            printf("Message forwarded to %s:%u\n", ip, successor_udp);
                        }

                        free(get_fwd);
                    }

                } else {
                    fprintf(stderr, "Unknown command: \"%s\"\n", input);

                    printf("Expected: \"PUT\" or \"GET\"\n");
                }

            }
        }



    }
}

int main(int argc, char*argv[]){
    
    // check argument format
    if (argc != 6){
        printf("Usage: %s <ip> <udp_port> <successor_udp_port> <tcp_port> <node_id>\n", argv[0]);
        return 1;
    }

    // check if the ip is valid
    if (is_valid_ip(argv[1]) < 1){
        if (is_valid_ip(argv[1]) < 0){
            printf("ipnet_pton or winsock error...idk\n");
        } else {
            printf("Invalid IP address\n");
        }
        return 1;
    }
    
    // check if the ports are valid
    if (atoi(argv[2]) < 1024 || atoi(argv[3]) < 1024 || atoi(argv[4]) < 1024){
        printf("Port numbers must be greater than 1024\n");
        return 1;
    } else if (atoi(argv[2]) > 65535 || atoi(argv[3]) > 65535 || atoi(argv[4]) > 65535){
        printf("Port numbers must be less than 65535\n");
        return 1;
    } else if (atoi(argv[2]) == atoi(argv[3]) || atoi(argv[2]) == atoi(argv[4]) || atoi(argv[3]) == atoi(argv[4])){
        printf("Port numbers must be unique\n");
        return 1;
    }

    // check if node_id is valid
    if (atoi(argv[5]) < 1 || atoi(argv[5]) > NODE_COUNT){
        printf("Nodes are to be a unique number from 1 - %i\n", NODE_COUNT);
        return 1;
    } 

    HashTable main_ht = *create_table();

    manage_node_communication(argv[1], atoi(argv[2]), atoi(argv[3]), atoi(argv[4]), atoi(argv[5]), &main_ht);

    return 0;
    
}
