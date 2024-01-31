package main

import (
	"fmt"
	"sync"
	"math"
	// "io"
	"time"
	// "os"
)


var curr_priority = 0.0  //server_id/ math.Pow(10, math.Log10(float64(num_nodes) + 1))
var highest_priority = map[string]float64{} // map of message_id to highest priority
var num_proposed = map[string]int{} // map of message_id to number of other priorities that have proposed a priority
var msgID_to_item = map[string]*Item{} // used to update messages in the queue
var received_messages = map[string]bool{} // used to keep track of all delivered messages, so messages are not delivered twice
var pq = &PriorityQueue{
	items: make([]*Item, 0),
	mutex: &sync.Mutex{},
}

var received_mu = sync.Mutex{}
var priority_mu = sync.Mutex{}
var failure_mu = sync.Mutex{}




/*

bMulticast to all other alive nodes in the group

*/

func bMulticast(msg Message) {
    for i := 0; i < num_nodes; i++ {
        if int64(i) != server_id && encoders[i] != nil {
			
			err := encoders[i].Encode(msg)
			if err != nil {
				// Failure detection for multicasting
				failure_mu.Lock()
				// consider adding to set of failed nodes
				if decoders[i] != nil {
					alive_nodes--
				}
				encoders[i] = nil
				decoders[i] = nil
				InChannels[i] = nil
				OutChannels[i] = nil
				msg.Error = true
				pq.update(msgID_to_item[msg.ID], msg, msg.Priority)
				failure_mu.Unlock()
				continue
			}
        } 
    }
}



/*

bDeliver ensures that all nodes receive a message once it is deliverable by multicasting to all other nodes if not seen before

*/

func bDeliver(msg Message) {
    _, exists := received_messages[msg.ID]
    if !exists {
		received_mu.Lock() 
        received_messages[msg.ID] = true
		received_mu.Unlock()
        if msg.FromNode != server_id {
            bMulticast(msg)
        }
        rDeliver(msg)
    }
}



/*

rDeliver updates the application with a transaction if it is valid

*/


func rDeliver(msg Message) {
	// End time:
	now := time.Now()
	deliveryTime := float64(now.UnixNano()) / float64(time.Second)
	msg_metrics := fmt.Sprintf("%s,%f,%f,%d,priority:%f\n", msg.ID, msg.StartTime, deliveryTime, server_id, msg.Priority)
	metrics_file.Write([]byte(msg_metrics))
    

	switch msg.Type {
    case "DEPOSIT":
       balances[msg.From] += msg.Amount
	   printAllBalances()

    case "TRANSFER":
        // check if transfer is possible
        if balances[msg.From] - msg.Amount >= 0 {
            balances[msg.From] -= msg.Amount
            balances[msg.To] += msg.Amount
			printAllBalances()
        }
    }    
}


/*

Function to pop messages from the front of the queue if they are deliverable or error

*/

func listenForDeliverable() {
	pq.mutex.Lock()
	for pq.Len() > 0 {
		item := pq.Peek()
		if item != nil && item.value.Deliverable {
			// deliver vaild items
			item = pq.PopItem()
			bDeliver(item.value)
		} else if item.value.Error {
			pq.PopItem()
		} else {
			break
		}
	}
	pq.mutex.Unlock()
}


/*

Function used to unicast message priority back to original sender 

*/

func sendProposedPriority(msg Message) {
	if encoders[msg.FromNode] != nil {
		err := encoders[msg.FromNode].Encode(msg)
		if err != nil {
			// update queue so undeliverable message will not block
			failure_mu.Lock()
			if decoders[msg.FromNode] != nil {
				alive_nodes--
			}
			encoders[msg.FromNode] = nil
			decoders[msg.FromNode] = nil
			msg.Error = true
			pq.update(msgID_to_item[msg.ID], msg, msg.Priority)
			failure_mu.Unlock()
			return
		}	
	}
	
}

/*

setPriority updates self priority queue for the first time a message is received at a node

*/

func setPriority(msg Message) (Message){
	priority_mu.Lock()
	msg.Priority = curr_priority + (float64(server_id) / 10)
	msg.Deliverable = false
	item := &Item{
		value:    msg,
		priority: msg.Priority,
	}

	curr_priority++
	msgID_to_item[msg.ID] = item
	highest_priority[msg.ID] = math.Max(highest_priority[msg.ID], msg.Priority)
	num_proposed[msg.ID]++
	pq.PushItem(item)
	priority_mu.Unlock()
	
	return msg
}


/*

This function is called the first time a transaction is read in from STDIN

*/

func isisMulticast(msg Message) {
	msg = setPriority(msg) // set own priority
	bMulticast(msg) // multicast to other nodes
}


/*

handleMessage takes in a received message as a parameter

// 3 cases:
// (1) The message is marked deliverable and originated at another node
// (2) The message is returned to the sender with a proposed priority
// (3) The message is received from a different node from the sender for the first time,
*/

func handleMessage(msg Message) {
	
	if msg.Deliverable == true { // (1)
		priority_mu.Lock()
		highest_priority[msg.ID] = msg.Priority
		curr_priority = math.Max(float64(math.Ceil(msg.Priority)), curr_priority)
		curr_priority++
		pq.update(msgID_to_item[msg.ID], msg, msg.Priority)
		priority_mu.Unlock()
		listenForDeliverable()

	} else { // (2)
		if msg.FromNode == server_id {
			// two cases: 
			if num_proposed[msg.ID] < alive_nodes {	
				priority_mu.Lock()
				num_proposed[msg.ID]++
				highest_priority[msg.ID] = math.Max(highest_priority[msg.ID], msg.Priority)
				priority_mu.Unlock()
			}  
			// multicast msg with final priorty to group
			if num_proposed[msg.ID] == alive_nodes {
				priority_mu.Lock()
				highest_priority[msg.ID] = math.Max(highest_priority[msg.ID], msg.Priority)
				msg.Priority = highest_priority[msg.ID]
				msg.Deliverable = true
				pq.update(msgID_to_item[msg.ID], msg, highest_priority[msg.ID]) // update self priority
				priority_mu.Unlock()

				listenForDeliverable()
				bMulticast(msg)
			}
		} else { // (3)
			msg = setPriority(msg)
			sendProposedPriority(msg)
		}
		
	}
 
}


/*

listenOnConnection is run by each node in the group to listen to incoming messges

*/
func listenOnConnection(idx int) {
    for {
        var msg Message

		if decoders[idx] != nil {
			err := decoders[idx].Decode(&msg)
			if err != nil {
				failure_mu.Lock()
				if encoders[idx] != nil {
					alive_nodes--
				}
				encoders[idx] = nil
				decoders[idx] = nil
				InChannels[idx] = nil
				OutChannels[idx] = nil
				failure_mu.Unlock()
				return
			}
			
			handleMessage(msg)	
		} else {
			return
		}
    }
}