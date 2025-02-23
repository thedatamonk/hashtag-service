package main

// in this approach, we will locally keep a copy of the counters of each tag
// and only after a certain time will write it to mongodb
// a local copy can be kept in a hashmap