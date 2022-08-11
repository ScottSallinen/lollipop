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

Try running a basic pagerank as async (-a), instead of the default sync, on the test graph:
```
lp-pagerank -a -g data/test.txt
```

Try running the dynamic version (-d), and check the result is the same as an async static run (-f).
```
lp-pagerank -d -f -g data/test.txt
```

There are more options for oracle comparisons, writing out properties, etc.

### Analyze Performance

A great tool for analyzing go programs is pprof. The web interface is a great quick way to view results.
As a basic example; while a graph is processing (you will want to use a larger graph, not the toy ones), check out memory usage by running:
```
go tool pprof -http=0.0.0.0:6061  http://localhost:6060/debug/pprof/heap
```
and then go to localhost:6061 in your browser. Check out sample->inuse_space, for example.

Check out some performance bottlenecks instead, with a 1 second CPU profile:
```
go tool pprof -http=0.0.0.0:6061  http://localhost:6060/debug/pprof/profile?seconds=1
```
Then check out things like view->souce to see where the bottlenecks are.

### Notes

If you make changes to the code, remember to rebuild with `go install ./...`

Get larger graphs from `https://snap.stanford.edu/data/` and try randomizing the edge list ordering with `lp-edgelist-shuffle`

### Why the name lollipop?

![](https://i.imgur.com/7eVa1Cp.png)

While most papers describe success, failures are often not mentioned... The lollipop is a graph structure
that represents a basic "proof by counterexample" about why certain algorithms do no function well with
deletion events.

Take for example the graph A->B->C->D->E->B. Suppose we wish to keep track of a basic
connected components; the initial state is that this graph is a single component. Suppose the "source" or "head"
of the component were vertex A. If we were to delete the edge between A->B, the question is, should vertex B
believe it is no longer in the "A" component? The simple answer, viewing the whole graph, is obviously yes.
However, when a vertex's view is only local, the question is more complex. B has an in-edge from E, which
told it that E has a connection to A and is thus E is in the A component. Thus, should B believe it has an alternate
path back to A through E? We know this to be untrue because this path includes B itself, but without passing the entire
historical derivation path, that is unknowable; if there *was* the edge from A->E, this would be a valid path
to keep B within the A component, but this is outside B's immediate local view.
But, suppose B pessimistically declares itself not in the A component, and passes this message to C, informing there is no
longer a path through it to A. However, concurrently, we add the edge D->B. In this case, D still believes to be a part of
component A, and informs B that it can now connect to A through it... which, will be wrong, but will cause B to believe it
has a valid path to A once again.

This logical counterexample of "easy" delete events show how care is needed with algorithm design.