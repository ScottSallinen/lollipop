## Lollipop
A testing / "simulation" framework for dynamic graph algorithms. 
The goal of the algorithm design is to be amenable to a distributed implementation, but to remain easy to reason and debug.

### Install

Install/update go. Suggested process for linux:
```
git clone https://github.com/udhos/update-golang
cd update-golang
sudo ./update-golang.sh
```

Clone this repo and build/install the framework. e.g.,
```
git clone https://github.com/ScottSallinen/lollipop.git
cd lollipop
go install ./...
```

### Run and Analyze Algorithm Results

Try running asynchronous pagerank on a test graph:
```
lp-pagerank -g data/test.txt
```

Try running the dynamic version (-d), and check the final result is the same as an async static run (-o).
```
lp-pagerank -g data/test.txt -d -o
```

There are many more options, check graph/graph-options.go

### Analyze Algorithm Implementation Performance

A great tool for analyzing go programs is pprof. The web interface is a great quick way to visualize results.

#### Whole Execution

For a thorough view, use the `-profile` flag. This will spit out an `algorithm.pprof` and a `stream.pprof` file for the whole execution, which you can view. Example:
```
go tool pprof -http=0.0.0.0:6061 algorithm.pprof
```

Head to the local hosted webpage, then check out things like view->source to see where the bottlenecks are.

#### Sampling

Enabling sampling is done with the flag like: `-pprof "0.0.0.0:6060"`
As a basic example; while a graph is processing (you will want to use a larger graph, not the toy ones), check out memory usage by running:
```
go tool pprof -http=0.0.0.0:6061  http://localhost:6060/debug/pprof/heap
```
and then go to localhost:6061 in your browser. Check out sample->inuse_space, for example.

Check out some performance bottlenecks instead, with a 1 second CPU profile:
```
go tool pprof -http=0.0.0.0:6061  http://localhost:6060/debug/pprof/profile?seconds=1
```

### Input Format

The system expects a "stream" of events. For now this is done with text files, that are like a line-by-line "event log" of edge changes.
Simple src, dst pairs, with option properties, will be assumed "add only".
```
src dst ...
```

Ideally these vertex identifiers are integers, but this is not required. For using string identifiers, see `stream-parse.go`

Additionally, unlike most other graph processing frameworks...
- Ordinal "zero indexed" data is not required.
- No pre-processing is required. 
- No a-priori information about the stream is required. (e.g., vertex counts or edge counts are not needed.)

For dealing with edge properties (including beyond weights and timestamps), see `graph-edge.go` for how to define edge parsing.

In `stream-parse.go` it can also be shown how to adapt to adjusting for input format relatively easily, such as formatting for a log like:
```
a src dst
d src dst
```
to represent addition and deletion events.

Finally, note the input graph is assumed to potentially be a multi-graph (multiple edges between vertices).
At the moment, this means simply defined deletions will remove the first edge that matches the pair (first in terms of order in the stream).

### Graphs to Test With

I suggest starting with the Wikipedia-Growth graph, which you can find here:
```http://konect.cc/networks/wikipedia-growth/```

Graphs from the PageRank paper can be found here (so long as I continue hosting!)
```https://greymass.ca/graphs/```

One can also find some graphs here:
```https://snap.stanford.edu/data/```

### Notes

If you make changes to the code, remember to rebuild with `go install ./...`

Try randomizing the edge list ordering of a graph with `lp-edgelist-tools`
But ensure you remove any comments or headers from your input graph file -- keep it just as [src, dst, ...] format.

`export GOAMD64=v3` might be good if you have a recent AMD CPU.

### Why the name Lollipop?

In graph theory, a [lollipop graph](https://mathworld.wolfram.com/LollipopGraph.html) is a connected graph formed by attaching a path graph or "stick" to a complete graph or "clique".

These particular graphs, when used a test input for asynchronous graph processing, represent an excellent scenario to test certain effects, and for certain attempts at describing an algorithm, represent a basic "proof by counterexample" about why certain algorithms do no function well with deletion events.
Take for example, the following example graph, which is being dynamically constructed to become a lollipop (edges from C<->E, or D<->B, are yet to be added).

![](https://i.imgur.com/7eVa1Cp.png)

The present graph is A->B->C->D->E->B. Suppose we wish to keep track of a basic connected components; the initial state is that this graph is a single component. Suppose the "source" or "head" of the component were vertex A. If we were to delete the edge between A->B, the question is, should vertex B believe it is no longer in the "A" component? The simple answer, viewing the whole graph, is obviously yes. However, when a vertex's view is only local, the question is more complex. B has an in-edge from E, which told it that E has a connection to A and is thus E is in the A component. Thus, should B believe it has an alternate path back to A through E? We know this to be untrue because this path includes B itself, but without passing the entire historical derivation path, that is unknowable; if there *was* the edge from A->E, this would be a valid path to keep B within the A component, but this is outside B's immediate local view. But, suppose B pessimistically declares itself not in the A component, and passes this message to C, informing there is no longer a path through it to A. However, concurrently, we add the edge D->B. In this case, D still believes to be a part of component A, and informs B that it can now connect to A through it... which, will be wrong, but will cause B to believe it has a valid path to A once again.

This proof by counterexample of implementing "easy" delete events show how care is needed with asynchronous algorithm design.


### Present Limitations

- This framework is meant for rapid prototyping, and as such, there are issues like shared memory being used for debug analysis. As such, without removing or overhauling these features, a distributed implementation is not yet available. We suggest [HavoqGT]([link](https://github.com/LLNL/havoqgt)) as an alternative platform for distributed analysis -- the programming interface is quite similar (though in C++) and designs are likely to be compatible.
- Using message aggregation may only be useful for algorithms that do not require precisely structured ordering for topology events mixed with algorithmic events (although, most algorithms thus far seem agnostic to this). For example, if there is an ADD(A, B), DEL(A, C), ADD(A, D), the message aggregation could pull information from all of these events into the first application of ADD(A, B), which, depending on the algorithm, may present 'non-causal' behaviour. (To fix this, the message aggregation could be a staged multi-queue, such that events that are deemed non-causal can be merged, and causal events change the stage of the queue to prevent merging before the causal event, but allow merging afterwards.. I haven't tried implementing this, so one can stick to avoiding aggregation if this is critical.)
- We assume the graph is NOT zero indexed. If it is, we could gain a lot of potential performance. Maybe make a flag that suggests the input is ordinal? A lot of performance is wasted on determining vidx <-> rawId.
- There is a lack of consideration for memory efficiency when generating and logging timeseries data...
- Making a graph undirected is not very performant, and could well be improved. I do have some ideas for this.. but directed graphs represent real-world "event" based systems better anyways (with some action having a cause and effect: source and destination...)
- Modifying an edge's property (e.g. weight) is not yet well supported. Mostly as this has some important considerations for multi-graphs. We could easily perform an update if the user provides the original timestamp of the target edge.. but only if that was given (and the stream is orderly).
- Deletes could be improved; they are not optimized well. Similarly, it may be interesting to allow more specificity in deletes for multi-graphs (right now the unspecific "delete the edge between A and B" deletes the first instance, and order is preserved).
- Moving deleted edges to a tertiary data structure would be very simple and enable complete historical analysis with temporal data, since all temporal ordering is preserved.
- Some constants defined in graph.go. I do miss CPP templates...
- Much more..
