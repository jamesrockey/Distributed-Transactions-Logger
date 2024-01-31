package main

import (
    "container/heap"
    // "fmt"
    "sync"
)

// An Item is something we manage in a priority queue.
type Item struct {
	value    Message // The value of the item; arbitrary.
	priority float64    // The priority of the item in the queue.
	index int // The index of the item in the heap.
}


// // update modifies the priority and value of an Item in the queue.
func (pq *PriorityQueue) update(item *Item, value Message, priority float64) {
	// pq.mutex.Lock()
	// defer pq.mutex.Unlock()
	item.value = value
	item.priority = priority
	heap.Fix(pq, item.index)
}



// Define a priority queue.
type PriorityQueue struct {
    items []*Item   // The items in the queue.
    mutex *sync.Mutex // A mutex to synchronize access to the queue.
}

// Define the methods for the priority queue.

func (pq *PriorityQueue) Len() int {
    return len(pq.items)
}

func (pq *PriorityQueue) Less(i, j int) bool {
    return pq.items[i].priority < pq.items[j].priority
}

func (pq *PriorityQueue) Swap(i, j int) {
    pq.items[i], pq.items[j] = pq.items[j], pq.items[i]
    pq.items[i].index = i
    pq.items[j].index = j
}

func (pq *PriorityQueue) Push(x any) {
    item := x.(*Item)
    item.index = len(pq.items)
    pq.items = append(pq.items, item)
}

func (pq *PriorityQueue) Pop() any {
    old := pq.items
    n := len(old)
    item := old[n-1]
    item.index = -1 // for safety
    pq.items = old[0 : n-1]
    return item
}

func (pq *PriorityQueue) Peek() *Item {
    if pq.Len() == 0 {
        return nil
    }
    return pq.items[0]
}

func (pq *PriorityQueue) PushItem(item *Item) {
    // pq.mutex.Lock()
    // defer pq.mutex.Unlock()
    heap.Push(pq, item)
}

func (pq *PriorityQueue) PopItem() *Item {
    // pq.mutex.Lock()
    // defer pq.mutex.Unlock()
    return heap.Pop(pq).(*Item)
}
