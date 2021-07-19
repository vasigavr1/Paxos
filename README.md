# CLASSIC PAXOS

This repo contains the implementation of Classic Paxos and All-aboard Paxos.
It is a submodule part of the [Odyssey](https://github.com/vasigavr1/Odyssey) project.
Please refer to the Odyssey README on how to compile and execute.
(As all Odyssey projects it has been tested and can be deployed on the [Cloudlab](https://cloudlab.us/)).

* cp_core (i.e. /include/cp_core & /src/cp_core) contain the implementation of the CP and All-aboard protocols.
  * cp_core_interface.h states how the rest of the repo interfaces with the protocol
* cp_netw contain the networking and kvs implementation that leverages cp_core and odlib to execute CP/All-aboard
    * i.e. the functions that must be registered with odyssey, such as the main_loop and the handlers to send/receive/insert messages & 
      propagate them to the KVS.
* cp_top includes common data structures (e.g. message types) and implements functionality such as initialization and statistics
      
To use All-aboard, enable it from /include/cp_config.h and recompile.
Other important knobs can be configured from od_top.h or be passed as input as described in the Odyssey Readme.


