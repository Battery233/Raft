# FALL2020-P2-RELEASE

# RAFT

This repository contains the starter code for project 2 (15-440/15-640, Fall 2020).
These instructions assume you have set your `GOPATH` to point to the repository's
root `P2/` directory.

## Starter Code

The starter code for this project is organized roughly as follows:

```

src/github.com/cmu440/        
  raft/                            Raft implementation, tests and test helpers

  rpc/                             RPC library that must be used for implementing Raft

```

## Instructions

##### How to Write Go Code

If at any point you have any trouble with building, installing, or testing your code, the article
titled [How to Write Go Code](https://golang.org/doc/gopath_code.html) is a great resource for understanding
how Go workspaces are built and organized. You might also find the documentation for the
[`go` command](http://golang.org/cmd/go/) to be helpful. As always, feel free to post your questions
on Piazza.

### Executing the official tests

#### 1. Checkpoint

To run the checkpoint tests, run the following from the src/github.com/cmu440/raft/ folder

```bash
go test -run 2A
```

#### 2. Full test

To execute all the tests, run the following from the src/github.com/cmu440/raft/ folder

```bash
go test
```

## Submission
Please disable or remove all debug prints regardless of whether you are using our logging
 framework or not before submitting to Gradescope. This helps avoid inadvertent failures 
 and messy autograder outputs and style point deduction. 


For both the checkpoint and the final submission, create `handin.zip` using the following 
command under the `P2/` directory, and then upload it to Gradescope. 
```
sh make_submit.sh
```

## Miscellaneous

### Reading the API Documentation

Before you begin the project, you should read and understand all of the starter code we provide.
To make this experience a little less traumatic (we know, it's a lot :P),
fire up a web server and read the documentation in a browser by executing the following command:

```sh
godoc -http=:6060 &
```

Then, navigate to [localhost:6060/pkg/github.com/cmu440](http://localhost:6060/pkg/github.com/cmu440) in a browser.
Note that you can execute this command from anywhere in your system (assuming your `GOPATH` is pointing to the project's root `P2/` directory).

