# RAFT

A Golang implementation of the Raft consensus protocol

The code for this project is organized roughly as follows:

```

src/github.com/cmu440/        
  raft/                            Raft implementation, tests and test helpers

  rpc/                             RPC library that must be used for implementing Raft

```