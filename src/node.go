/*
Distributed Transaction Logger
CS 425 MP1

James Rockey (jrockey2)

This project is a distributed system consisting of multiple nodes that maintain
transaction logs for a financial application. The goal of the system is to ensure
that all transactions are recorded consistently across all nodes, even in the event
of node failures or network partitions. Each node maintains a local copy of the
transaction log, and logs are replicated across nodes using a total ordering algorithm
to ensure that they remain consistent.

In order to maintain consistency across all nodes, the system uses the ISIS Total Ordering
Algorithm to ensure that all logs remain consistent. When a node receives a new
transaction, it proposes it to the other nodes in the system using the ISIS Total Ordering Algorithm. 
Once the proposal is accepted by all of the nodes, the transaction is committed 
to the logs and replicated to all other nodes in the system.

This system is designed to be fault-tolerant and highly available, even in the
face of node failures or network partitions. If a node fails or becomes unreachable,
the other nodes in the system can continue to function normally and maintain
consistency among their logs.
*/


package main

import (
	"fmt"
	"bufio"
    "net"
	"strings"
	"strconv"
	"os"
    "encoding/gob"
    "sort"
    "sync"
    "time"
)


type Node struct {
    ID       int
    Name     string
    Address string
    Port     string   
}

var num_nodes = 0 
var alive_nodes = 0 // used to track current progress of the system
var num_connections_received = 0 // used to ensure that system is fully connected before setup
var num_connections_sent = 0
var messages_sent = 0 // used for Message.ID


var connect_mutex = sync.Mutex{}

var server_id = int64(-1)
var server_name = ""
var server_address = ""
var server_port = ""
var InChannels []*net.Conn // array of connections to every other node in the system, indexed by server_id
var encoders []*gob.Encoder
var OutChannels []*net.Conn
var decoders []*gob.Decoder



// Current balances of people in the sytem
var balances = map[string]int64{}


// Used for graphs and testing total ordering
var file *os.File
var metrics_file *os.File


type Message struct {
    ID         string  // {server_name} + '-' + {messages_sent}
    FromNode   int64
    Type       string
    From       string
    To         string
    Amount     int64
    Deliverable bool
    Error       bool
    Priority   float64
    StartTime   float64
}



/*

Parses config file to find information about nodes in the system

*/

func readConfig(filename string) ([]Node, error) {
    file, err := os.Open(filename)

	if err != nil {
        // fmt.Printf("Error reading configuration file %s: %v\n", filename, err)
        return nil, nil
    }
    defer file.Close()

    scanner := bufio.NewScanner(file)

	scanner.Scan()
    num_nodes, err = strconv.Atoi(scanner.Text())
    alive_nodes = num_nodes
    if err != nil {
        fmt.Printf("Error reading number of nodes from configuration file: %v\n", err)
        return nil, nil
    }

	nodes := make([]Node, num_nodes)
    for i := 0; i < num_nodes; i++ {
        scanner.Scan()
        fields := strings.Fields(scanner.Text())
        if len(fields) != 3 {
            fmt.Printf("Error reading node information from configuration file: expected 3 fields, but got %d\n", len(fields))
            return nil, nil
        }

        if err != nil {
            fmt.Printf("Error reading node port from configuration file: %v\n", err)
            return nil, nil
        }

        nodes[i] = Node{
            ID:       i,
            Name:     fields[0],
            Address: fields[1],
            Port:     fields[2],
        }
        if nodes[i].Name == server_name {
            server_id = int64(nodes[i].ID) // received help from TA, but probably not necessary to do this
            server_address = nodes[i].Address
            server_port = nodes[i].Port
        }
    }
    InChannels = make([]*net.Conn, num_nodes)
    OutChannels = make([]*net.Conn, num_nodes)
    encoders = make([]*gob.Encoder, num_nodes)
    decoders = make([]*gob.Decoder, num_nodes)
    return nodes, nil
}



func listenForConnections(listener net.Listener) {
    // wait for all other nodes to connect to server
    for num_connections_received < num_nodes - 1 {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println(err)
			continue
		}
        
        var msg Message 
        decoder := gob.NewDecoder(conn)     
        err = decoder.Decode(&msg)

        if err != nil {
            continue
        }

        InChannels[msg.FromNode] = &conn
        decoders[msg.FromNode] = decoder
        num_connections_received += 1	
	}
}

func connectToNode(node *Node) {
    for {
        // Connect to the server
        conn, err := net.Dial("tcp", node.Address+":"+node.Port)
        if err != nil {
            continue
        }
        connect_mutex.Lock()

        msg := Message{}
        msg.FromNode = server_id
        OutChannels[node.ID] = &conn
        encoders[node.ID] = gob.NewEncoder(conn)
        err = encoders[node.ID].Encode(msg)
        if err != nil {
            connect_mutex.Unlock()
            continue
        }

        num_connections_sent++
        connect_mutex.Unlock()
        return
    }
}



/*
This function parses a string input and turns into a message
*/

func ParseMessage(msg string) (Message, error) {
    parts := strings.Split(msg, " ")
    if len(parts) != 3 && len(parts) != 5 {
        return Message{}, fmt.Errorf("invalid message format")
    }
    
    m := Message{}

    m.ID = server_name + "-" + strconv.Itoa(messages_sent) // message id: node1-0
    messages_sent++
    m.FromNode = server_id

    switch parts[0] {
    case "DEPOSIT":
        m.Type = "DEPOSIT"
        m.From = parts[1]
        amount, err := strconv.Atoi(parts[2])
        if err != nil {
            return m, err
        }
        m.Amount = int64(amount)
        
    case "TRANSFER":
        m.Type = "TRANSFER"
        m.From = parts[1]
        m.To = parts[3]
        amount, err := strconv.Atoi(parts[4])
        if err != nil {
            return m, err
        }
        m.Amount = int64(amount)
        
    default:
        return m, fmt.Errorf("invalid message type")
    }

    now := time.Now()
	startTime := float64(now.UnixNano()) / float64(time.Second)
    m.StartTime = startTime
    return m, nil
}


/*

This function prints all the current non zero balances in the system in alphabetical order

*/

func printAllBalances() {
 
    var accounts []string
    for key := range balances {
        accounts = append(accounts, key)
    }
    sort.Strings(accounts)
    fmt.Printf("BALANCES")
    file.Write([]byte("BALANCES"))
    for _, account := range accounts {
        if balances[account] > 0 {
            fmt.Printf(" %s:%d", account, balances[account])
            msg := fmt.Sprintf(" %s:%d", account, balances[account])
            file.Write([]byte(msg))
        }
    }
    fmt.Printf("\n")
    file.Write([]byte("\n"))
}

func send_message(line string) {
    msg, err := ParseMessage(line)
    if err == nil {
        isisMulticast(msg)
    }
}






func main() {
    file, _ = os.OpenFile("testdiff.txt", os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
    metrics_file, _ = os.OpenFile("system_metrics.txt", os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
    defer file.Close()
    defer metrics_file.Close()



    
    server_name = os.Args[1]
    configFile := os.Args[2] // This file contains the name, address, and port of each node in the distributed system
    // fmt.Println(server_name)
	// configFile := "config.yaml"
	nodes, _ := readConfig(configFile)

    if server_id == -1 { // error parsing the config file
        return
    }

   
    listener, err := net.Listen("tcp", ":" + server_port)
	if err != nil {
		panic(err)
	}
	defer listener.Close()

    
    // listen for all incoming nodes and call all other nodes
    // call one go routine to listen to incoming connections, and returns after n-1 nodes have connected
    go listenForConnections(listener)

    // connect to all other nodes in the system
    for i := 0; i < num_nodes; i++ {
        if int64(i) != server_id {
            go connectToNode(&nodes[i])
        } else {
            // fmt.Println(i)
        }
    }
    

    // wait for all other nodes to be connected to server
    for (num_connections_received < num_nodes - 1) || (num_connections_sent < num_nodes - 1) {
    

    }

    fmt.Println("All Nodes Connected")


    // listen to all other nodes
    for i := 0; i < num_nodes; i++ {
        if int64(i) != server_id {
            go listenOnConnection(i)
        } 
    }

      

    // read input from stdin
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
        line := scanner.Text()
        send_message(line)
        
	}
}