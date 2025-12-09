Distributed Grep with the MapReduce Framework
=============================================

This repository already contains a coordinator/worker MapReduce runtime (`src/mr`). The new `mrapps/grep.go` plugin lets you run a distributed `grep` that scans many input files in parallel.

Quick start (from the `src` directory)
--------------------------------------
- Build the grep plugin: `go build -buildmode=plugin -o mrapps/grep.so mrapps/grep.go`
- Start the coordinator with your input files (choose any `pg-*.txt` or your own): `go run main/mrcoordinator.go main/pg-*.txt`
- In one or more other shells, launch workers with the pattern to search for: `MR_GREP_PATTERN="Sherlock Holmes" go run main/mrworker.go mrapps/grep.so`
- When the workers finish, inspect the results: `cat mr-out-*`

How it works
------------
- Workers read each file line by line and emit matches where the line contains `MR_GREP_PATTERN`.
- Each match is recorded as `<filename>:<line number>:<line contents>`; the reduce step sorts all matches for stable output.
- `MR_GREP_PATTERN` must be set for every worker process (coordinator does not need it).