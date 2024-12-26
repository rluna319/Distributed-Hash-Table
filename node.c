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

typedef enum {
    PUT_FORWARD,
    GET_FORWARD,
    WHAT_X,
    GET_REPLY_X,
    PUT_REPLY_X
} MsgType;

typedef struct app_msg{
    MsgType msg_id;
    unsigned int key;
    unsigned int value;
    char * ip_addr;
    unsigned int tcp_port;
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
    unsigned int index = table_hash(key);

    // create the new node
    HT_Node *new_node = create_node(key, value);

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
    unsigned int index = table_hash(key);

    HT_Node *current = table->buckets[index];

    while (current != NULL){
        if (current->key == key){
            return current->value;
        }
        current = current->next;
    }
    return UINT_MAX;    // key not found retrun max value for uint as sentinel value
}

static void delete(HashTable *table, const unsigned int *key){
    unsigned int index = table_hash(key);

    HT_Node *current = table->buckets[index];
    HT_Node *prev = NULL;

    while (current != NULL && current->key != key){ // search till NULL or key is found
        prev = current;
        current = current->next;
    }

    if (current == NULL){ // didn't find it
        printf("Key not found : %s\n", key);
        return;
    }

    if (prev == NULL){  // found it (is it the head?)
        table->buckets[index] = current->next;
    } else {    // not the head
        prev->next = current->next;
    }

    free(current->key);
    free(current->value);
    free(current);
    printf("Key deleted: %s`n", key);

    return;
}
/* End Hash Table */









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

static unsigned long ip_to_int(const char *ip_str){
    struct sockaddr_in sa;

    #ifdef _WIN32
        WSADATA wsaData;
        if (WSAStartup(MAKEWORD(2, 2), &wsaData) != 0){
            printf("WSAStartup failed\n");
            return 0;
        }
    #endif

    // validate and conver the ip to binary format
    if (inet_pton(AF_INET, ip_str, &(sa.sin_addr)) != 1){
        #ifdef _WIN32
            WSACleanup();
        #endif
        return 0;
    }

    #ifdef _WIN32
        WSACleanup();
    #endif

    // conver the binary ip to an integer
    return ntohl(sa.sin_addr.s_addr);   // convert to host byte order (inet_pton returns in network byte order)
}

















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
    int opt = 1, tcp_comm_sock_fd = 0;
    if (setsockopt(tcp_sock_fd, SOL_SOCKET, SO_REUSEADDR, (char *)&opt, sizeof(opt)) < 0){
        printf("TCP socket creation failed for multiple connectinos\n");
        exit(EXIT_FAILURE);
    }
    
    // file descriptor set
    fd_set readyfds, masterfds;

    // server and client credentials for tcp and udp
    struct sockaddr_in tcp_server_addr, tcp_client_addr, udp_server_addr, udp_client_addr;

    // specify server info
    tcp_server_addr.sin_family = AF_INET;
    udp_server_addr.sin_family = AF_INET;
    tcp_server_addr.sin_port = htons(tcp_port);
    udp_server_addr.sin_port = htons(udp_port);
    tcp_server_addr.sin_addr.s_addr = htonl(ip);
    udp_server_addr.sin_addr.s_addr = htonl(ip);

    int addr_len = sizeof(struct sockaddr);

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

    while(1){

        int key = 0, value = 0;

        readyfds = masterfds;   // copy master set of FDs (select will remove any that aren't "ready") into set of ready FDs for select to filter through.

        printf("Blocked on select system call...\n");

        int max_fd = udp_sock_fd > tcp_sock_fd ? udp_sock_fd : tcp_sock_fd;
        select(max_fd + 1, &readyfds, NULL, NULL, NULL);

        /*****************************************************************/
        /*                      TCP NEW CONNECTION                       */
        /*****************************************************************/

        if (FD_ISSET(tcp_sock_fd, &readyfds)){   // connection request received from client

            accept(tcp_sock_fd, (struct sockaddr *)&tcp_client_addr, &addr_len);

            if (tcp_comm_sock_fd < 0){
                printf("Accept Error: %d\n", errno);
                exit(0);
            }

            printf("Connection accepted from client: %s:%u\n", inet_ntoa(tcp_client_addr.sin_addr), ntohs(tcp_client_addr.sin_port));

            // add the new connectino socket to the active set
            FD_SET(tcp_comm_sock_fd, &masterfds);
            if (tcp_comm_sock_fd > max_fd){
                max_fd = tcp_comm_sock_fd;  // update the max file descriptor
            }
        } 

        /*****************************************************************/
        /*                    TCP/UDP COMMUNICATION                      */
        /*****************************************************************/

        // loop through all FDs
        for (int fd = 0; fd <= max_fd; fd++){
            
            /*** UDP COMMUNICATION ***/

            if (fd == udp_sock_fd && FD_ISSET(fd, &readyfds)){  

                char buffer[sizeof(app_msg_t)];
                struct sockaddr_in sender_addr;
                socklen_t addr_len = sizeof(sender_addr);

                ssize_t received_len = recvfrom(udp_sock_fd, buffer, sizeof(buffer), 0, (struct sockaddr *)&sender_addr, &addr_len);

                if (received_len < 0){
                    perror("Error receiving data\n");
                } else if (received_len != sizeof(app_msg_t)){
                    printf("Invalid message size: %zd bytes (expected %zu bytes)\n");
                } else {

                    // deserialize the buffer into the app_msg_t structure
                    app_msg_t *msg = (app_msg_t *)buffer;

                    // convert fields from network byte order to host byte order
                    msg->msg_id = ntohl(msg->msg_id);
                    msg->key = ntohl(msg->key);
                    msg->value = ntohl(msg->value);
                    msg->ip_addr = ntohl(msg->ip_addr);
                    msg->tcp_port = ntohl(msg->tcp_port);

                    // print to console
                    printf("Received Message:\n");
                    printf("  Msg ID:    %u\n", msg->msg_id);
                    printf("  Key:       %u\n", msg->key);
                    printf("  Value:     %u\n", msg->value);
                    printf("  IP Addr:   %s\n", inet_ntoa(*(struct in_addr *)&msg->ip_addr));
                    printf("  TCP Port:  %u\n", msg->tcp_port);
                    // Print sender details
                    printf("Message received from %s:%d\n", inet_ntoa(sender_addr.sin_addr), ntohs(sender_addr.sin_port));

                    // process message
                    if (msg->msg_id == PUT_FORWARD){   // PUT_FORWARD msg received from another node

                        if (node_hash(msg->key) == node_id){    // key is destined for this node

                            /**
                             * Need to send WHAT_X over TCP to the originator to get the value
                             * Must establish TCP connection with originator
                             */

                            // Initialize variables and specify server credentials
                            struct sockaddr_in dest;
                            memset(&dest, 0, sizeof(dest));
                            dest.sin_family = AF_INET;
                            dest.sin_port = htons(tcp_port);

                            // Resolve the hostname to an IP address
                            struct hostent *host = gethostbyname(msg->ip_addr);
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
                                exit(EXIT_FAILURE);
                            }

                            printf("Connection established with server: %s:%u\n",
                                inet_ntoa(dest.sin_addr), ntohs(dest.sin_port));

                            // Add the new connection socket to the active FD set
                            FD_SET(sockfd, &masterfds);
                            if (sockfd > max_fd) {
                                max_fd = sockfd;  // Update the max file descriptor
                            }

                            // prepare data
                            app_msg_t *what_x = malloc(sizeof(app_msg_t));
                            what_x->msg_id = htons(WHAT_X);
                            what_x->key = htons(msg->key);
                            what_x->value = NULL;       // don't have it
                            what_x->ip_addr = NULL;     // connect() will provide this
                            what_x->tcp_port = NULL;    // connect() will provide this

                            // send data
                            ssize_t sent_recv_bytes = sendto(sockfd, &what_x, sizeof(app_msg_t), 0, (struct sockaddr *)&dest, sizeof(struct sockaddr));

                            if (sent_recv_bytes < 0){
                                perror("WHAT_X send failed.\n");
                            } else {
                                printf("WHAT_X sent to %s:%u. %d bytes sent...\n", msg->ip_addr, msg->tcp_port, sent_recv_bytes);
                            }

                            free(what_x);   // free malloc

                        } else {    // PUT_FORWARD to successor node over UDP

                            // set up receiver address
                            struct sockaddr_in receiver_addr;
                            memset(&receiver_addr, 0, sizeof(receiver_addr));
                            receiver_addr.sin_family = AF_INET;
                            receiver_addr.sin_port = htons(successor_udp);
                            receiver_addr.sin_addr.s_addr = inet_addr(ip);

                            // set up message
                            app_msg_t *put_fwd = malloc(sizeof(app_msg_t));
                            put_fwd->msg_id = PUT_FORWARD;
                            put_fwd->key = key;
                            put_fwd->value = NULL;      // value remains "secret" until TCP connection established with target node
                            put_fwd->ip_addr = ip;
                            put_fwd->tcp_port = tcp_port;

                            // send message
                            ssize_t sent_bytes = sendto(udp_sock_fd, put_fwd, sizeof(app_msg_t), 0, (struct sockaddr *)&receiver_addr, sizeof(receiver_addr));

                            if (sent_bytes < 0){
                                perror("PUT_FORWARD send failed\n");
                            } else {
                                printf("Message forwarded to %s:%u\n", ip, successor_udp);
                            }

                            free(put_fwd);

                        }

                    } else if (msg->msg_id == GET_FORWARD){    // GET_FORWARD msg received from another node
                        
                        if (node_hash(msg->key) == node_id){
                            
                            // send GET_REPLY_X to originator over TCP

                            // Initialize variables and specify server credentials
                            struct sockaddr_in dest;
                            memset(&dest, 0, sizeof(dest));
                            dest.sin_family = AF_INET;
                            dest.sin_port = htons(tcp_port);

                            // Resolve the hostname to an IP address
                            struct hostent *host = gethostbyname(msg->ip_addr);
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
                                exit(EXIT_FAILURE);
                            }

                            printf("Connection established with server: %s:%u\n",
                                inet_ntoa(dest.sin_addr), ntohs(dest.sin_port));

                            // Add the new connection socket to the active FD set
                            FD_SET(sockfd, &masterfds);
                            if (sockfd > max_fd) {
                                max_fd = sockfd;  // Update the max file descriptor
                            }

                            // prepare data
                            app_msg_t *get_reply_x = malloc(sizeof(app_msg_t));
                            get_reply_x->msg_id = htons(GET_REPLY_X);
                            get_reply_x->key = htons(msg->key);
                            get_reply_x->value = htons(msg->value);
                            get_reply_x->ip_addr = NULL;     // connect() will provide this
                            get_reply_x->tcp_port = NULL;    // connect() will provide this

                            // send data
                            ssize_t sent_recv_bytes = sendto(sockfd, &get_reply_x, sizeof(app_msg_t), 0, (struct sockaddr *)&dest, sizeof(struct sockaddr));

                            if (sent_recv_bytes < 0){
                                perror("GET_REPLY_X send failed.\n");
                            } else {
                                printf("GET_REPLY_X sent to %s:%u. %d bytes sent...\n", msg->ip_addr, msg->tcp_port, sent_recv_bytes);
                            }

                        } else {    // GET_FORWARD to successor node over UDP

                            // set up receiver address
                            struct sockaddr_in receiver_addr;
                            memset(&receiver_addr, 0, sizeof(receiver_addr));
                            receiver_addr.sin_family = AF_INET;
                            receiver_addr.sin_port = htons(successor_udp);
                            receiver_addr.sin_addr.s_addr = inet_addr(ip);

                            // set up message
                            app_msg_t *get_fwd = malloc(sizeof(app_msg_t));
                            get_fwd->msg_id = GET_FORWARD;
                            get_fwd->key = key;
                            get_fwd->value = NULL;      // value doesn't get stored here as it is unknown
                            get_fwd->ip_addr = ip;
                            get_fwd->tcp_port = tcp_port;

                            // send message
                            ssize_t sent_bytes = sendto(udp_sock_fd, get_fwd, sizeof(app_msg_t), 0, (struct sockaddr *)&receiver_addr, sizeof(receiver_addr));

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
                }

            /*** TCP COMMUNICATION ***/

            } else if (fd != tcp_sock_fd && fd != udp_sock_fd && FD_ISSET(fd, &readyfds)){

                // prepare message buffer
                char buffer[sizeof(app_msg_t)];
                memset(buffer, 0, sizeof(buffer));

                // store client socket information
                struct sockaddr_in sender_addr;
                socklen_t addr_len = sizeof(sender_addr);

                // get message
                ssize_t received_len = recvfrom(fd, buffer, sizeof(buffer), 0, (struct sockaddr *)&sender_addr, &addr_len);

                // print info to console
                printf("Server received %d bytes from client %s: %u\n", received_len, inet_ntoa(sender_addr.sin_addr), ntohs(sender_addr.sin_port));

                // deserialize the buffer into the app_msg_t structure
                app_msg_t *msg = (app_msg_t *)buffer;

                // convert fields from network byte order to host byte order
                msg->msg_id = ntohl(msg->msg_id);
                msg->key = ntohl(msg->key);
                msg->value = ntohl(msg->value);
                msg->ip_addr = ntohl(msg->ip_addr);
                msg->tcp_port = ntohl(msg->tcp_port);

                // print message to console
                printf("Received Message:\n");
                printf("  Msg ID:    %u\n", msg->msg_id);
                printf("  Key:       %u\n", msg->key);
                printf("  Value:     %u\n", msg->value);
                printf("  IP Addr:   %s\n", inet_ntoa(*(struct in_addr *)&msg->ip_addr));
                printf("  TCP Port:  %u\n", msg->tcp_port);

                // if server receives emtpy message from client, server may close the conection and wait for fresh new connection from client.
                if (received_len == 0){
                    close(fd);
                    break;
                }

                // handle tcp related messages
                if (msg->msg_id ==  WHAT_X){ // send back PUT_REPLY_X

                    // double check we have the key value pair
                    if (search(hash_table, key) != NULL){   // if we do
                        
                        // prepare data
                        app_msg_t *put_reply_x = malloc(sizeof(app_msg_t));
                        put_reply_x->msg_id = htons(PUT_REPLY_X);
                        put_reply_x->key = htons(msg->key);
                        put_reply_x->value = htons(search(hash_table, key));
                        put_reply_x->ip_addr = NULL;
                        put_reply_x->tcp_port = NULL;

                        // send data
                        int sent_recv_bytes = sendto(fd, (char *)&put_reply_x, sizeof(put_reply_x), 0, (struct sockaddr *)&sender_addr, sizeof(struct sockaddr));

                        if (sent_recv_bytes < 0){
                            perror("PUT_REPLY_X failed...\n");
                        } else {
                            printf("PUT_REPLY_X sent to %s:%u\n", inet_ntoa(sender_addr.sin_addr), ntohs(sender_addr.sin_port));
                        }

                        free(put_reply_x);  // free the malloc
                    }

                } else if (msg->msg_id == GET_REPLY_X){

                    printf("GET request succeeded for Key = %i. Value = %i\n", msg->key, msg->value);

                } else if (msg->msg_id == PUT_REPLY_X){

                    // double check we are the correct node
                    if (node_hash(msg->key) == node_id){
                        insert(hash_table, msg->key, msg->value);
                        printf("Inserted new key-value pair for Key = %i\n", msg->key);
                    } else {
                        printf("Received PUT_REPLY_X for key = %i, but this is not the correct node for storage...\n", msg->key);
                    }

                } else {
                    perror("Invalid message type for TCP communication received...\n");
                    break;
                }

            }

        }
        
        /*****************************************************************/
        /*                      PUT/GET via Console                      */
        /*****************************************************************/

        if (FD_ISSET(0, &readyfds)){  // data arrives from console (user input)
            
            char input[256];
            if (fgets(input, sizeof(input), stdin) != NULL){

                if (strncmp(input, "PUT", 3) == 0){
                    
                    printf("Enter key: ");
                    fgets(atoi(key), sizeof(key), stdin);
                    while (key < 0){
                        printf("Must be a positive number...\n");
                        printf("Enter key: ");
                        fgets(atoi(key), sizeof(key), stdin);
                    }
                    printf("Enter value: ");
                    fgets(atoi(value), sizeof(value), stdin);

                    if (node_hash(key) == node_id){    // store key and value in own hash table
                    
                        insert(&hash_table, key, value);
                    
                    } else { // send PUT_FORWARD

                        // set up receiver address
                        struct sockaddr_in receiver_addr;
                        memset(&receiver_addr, 0, sizeof(receiver_addr));
                        receiver_addr.sin_family = AF_INET;
                        receiver_addr.sin_port = htons(successor_udp);
                        receiver_addr.sin_addr.s_addr = inet_addr(ip);

                        // set up message
                        app_msg_t *msg = malloc(sizeof(app_msg_t));
                        msg->msg_id = PUT_FORWARD;
                        msg->key = key;
                        msg->value = NULL;      // value remains "secret" until TCP connection established with target node
                        msg->ip_addr = ip;
                        msg->tcp_port = tcp_port;

                        // send message
                        ssize_t sent_bytes = sendto(udp_sock_fd, msg, sizeof(app_msg_t), 0, (struct sockaddr *)&receiver_addr, sizeof(receiver_addr));

                        if (sent_bytes < 0){
                            perror("PUT_FORWARD send failed\n");
                        } else {
                            printf("Message forwarded to %s:%u\n", ip, successor_udp);
                        }

                        free(msg);
                    }

                } else if (strncmp(input, "GET", 3) == 0){
                    
                    printf("Enter key: ");
                    fgets(atoi(key), sizeof(key), stdin);
                    while (key < 0){
                        printf("Must be a positive number...\n");
                        printf("Enter key: ");
                        fgets(atoi(key), sizeof(key), stdin);
                    }

                    if (node_hash(key) == node_id){ // if this node is holding the key specified in the GET request from console

                        printf("Value for key = %i found in memory. Value = %i", key, search(hash_table, key));

                    } else { // send GET_FORWARD

                        // set up receiver address
                        struct sockaddr_in receiver_addr;
                        memset(&receiver_addr, 0, sizeof(receiver_addr));
                        receiver_addr.sin_family = AF_INET;
                        receiver_addr.sin_port = htons(successor_udp);
                        receiver_addr.sin_addr.s_addr = inet_addr(ip);

                        // set up message
                        app_msg_t *msg = malloc(sizeof(app_msg_t));
                        msg->msg_id = GET_FORWARD;
                        msg->key = key;
                        msg->value = NULL;      // value doesn't get stored here as it is unknown
                        msg->ip_addr = ip;
                        msg->tcp_port = tcp_port;

                        // send message
                        ssize_t sent_bytes = sendto(udp_sock_fd, msg, sizeof(app_msg_t), 0, (struct sockaddr *)&receiver_addr, sizeof(receiver_addr));

                        if (sent_bytes < 0){
                            perror("GET_FORWARD send failed\n");
                        } else {
                            printf("Message forwarded to %s:%u\n", ip, successor_udp);
                        }

                        free(msg);

                    }

                } else {
                    perror("Unknown command: \"%s\"\n");

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
    if (atoi(argv[5]) > 1 - NODE_COUNT){
        printf("Nodes are to be uniquely number from 0 - %i\n", NODE_COUNT);
        return 1;
    } 

    HashTable main_ht = *create_table();

    manage_node_communication(argv[1], atoi(argv[2]), atoi(argv[3]), atoi(argv[4]), atoi(argv[5]), &main_ht);

    return 0;
    
}
