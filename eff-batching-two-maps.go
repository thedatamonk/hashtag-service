package main

// we maintain two maps in this approach - active and passive
// the messages are currently being written to the active map
// when it's full, we will swap the maps and start writing to the passive map
// ma, mp = mp, ma
// So now mp(ma) is being used to update the DB
// while ma(mp) is being used to collect the messages
// this way we can avoid locking the map for a longer time
