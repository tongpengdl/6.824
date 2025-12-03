**Implementation Plan:**

1.  **When a task sends an RPC asking for a task (The Strategy):**
    * **Coordinator:** Maintain a state for every task (`Idle`, `InProgress`, `Completed`). Hand out `Idle` Map tasks first.
    * **Barrier:** Wait until **all** Map tasks are `Completed` before handing out any Reduce tasks. If Maps are still running, tell workers to wait/sleep.

2.  **mapf (Worker Map Task):**
    * Read the input file and run the user's Map function to get KV pairs.
    * **Partition:** Loop through pairs and hash the key (`ihash(key) % N`) to pick one of $N$ buckets.
    * **Write:** Buffer these pairs and write them to $N$ separate intermediate files (e.g., `mr-MapID-ReduceID`) using JSON.

3.  **reducef (Worker Reduce Task):**
    * **Read:** Identify your assigned Reduce ID (e.g., `Y`). Read **all** intermediate files that end in `-Y` (from every Map task).
    * **Sort & Run:** Load all keys into memory, sort them by key, and feed each unique key and its values into the user's Reduce function.
    * **Output:** Write the final result to `mr-out-Y`.

4.  **Fault Tolerance (Coordinator Background Loop):**
    * Periodically check `InProgress` tasks. If a task started >10 seconds ago and hasn't reported back, reset it to `Idle` so another worker can pick it up.

**Next Step:** Shall we start by writing the `Coordinator` struct to track these task states?