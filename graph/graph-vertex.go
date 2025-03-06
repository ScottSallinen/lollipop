package graph

// Defines a vertex in a graph.
// Data here is thread-local, inaccessible to others, and persistent.
type Vertex[V VPI[V], E any] struct {
	OutEdges []Edge[E] // Main outgoing edgelist.
}

// Vertex structural properties. Handled and used by the construction process.
type VertexStructure struct {
	PendingIdx  uint64  // Used as offset data for dynamic construction.
	CreateEvent uint64  // Event index of the first event that created this vertex.
	InEventPos  uint32  // Used to calculate Pos for new in edges. Pro tip: this position is constant across executions (for the pos of the raw vertex that has an event to me), however note said vertex may have a different internal ID across executions.
	RawId       RawType // Raw (external) ID of the vertex. Pro tip: this is constant across executions. Internal IDs that graph threads choose for a given raw is NOT.
}

// Vertex Property Interface.
type VPI[V any] interface {
	New() (new V) // "Default constructor."
}

// Optional definition if the oracle requires copied properties from the given graph, before running the algorithm.
type VPCopyOracle[V any] interface {
	CopyForOracle(*V, *V)
}

// For Intra-Node communication. Represents an abstract mailbox system.
// Data here is ephemeral.
type VertexMailbox[M MVI[M]] struct {
	Inbox    M     // Mail for the vertex.
	Activity int32 // Indicates if the vertex is activated (e.g. has notification/mail). Used for either: unique notifications, or the number of notifications currently in the queue for it.
}

// Mailbox Value Interface.
type MVI[M any] interface {
	New() (new M) // "Default constructor."
}

type Notification[N any] struct {
	Note     N
	Target   uint32
	Activity int32
}

//type NVI[N any] interface {
//	New() (new N)
//}

// Computes offsets for buckets.
func idxToBucket(idx uint32) (b, p uint32) {
	p = idx & BUCKET_MASK
	b = idx >> BUCKET_SHIFT
	return b, p
}

// Expands an internal index into the thread-local index, and the responsible thread id.
func InternalExpand(internalId uint32) (idx, tidx uint32) {
	idx = internalId & THREAD_MASK
	tidx = internalId >> THREAD_SHIFT
	return idx, tidx
}

// Checks if two internal IDs are on the same graph thread.
func SameTidx(internalId uint32, otherInternalId uint32) bool {
	return (internalId & THREAD_ID_MASK) == (otherInternalId & THREAD_ID_MASK)
}

// ------------------ Thread level functions ------------------ //

// Wrapper for getting a vertex; okay to provide a vidx or just the thread-local offset.
func (gt *GraphThread[V, E, M, N]) Vertex(internalOrOffset uint32) *Vertex[V, E] {
	return &gt.Vertices[(internalOrOffset & THREAD_MASK)]
}

func (gt *GraphThread[V, E, M, N]) VertexProperty(internalOrOffset uint32) *V {
	idx := internalOrOffset & THREAD_MASK
	bucket, bpos := idxToBucket(idx)
	return &gt.VertexProperties[bucket][bpos]
}

// Wrapper for getting a vertex and its mailbox; okay to provide a vidx or just the thread-local offset.
func (gt *GraphThread[V, E, M, N]) VertexAndMailbox(internalOrOffset uint32) (*Vertex[V, E], *VertexMailbox[M]) {
	idx := internalOrOffset & THREAD_MASK
	bucket, bpos := idxToBucket(idx)
	return &gt.Vertices[idx], &gt.VertexMailboxes[bucket][bpos]
}

// Wrapper for getting a vertex mailbox; okay to provide a vidx or just the thread-local offset.
func (gt *GraphThread[V, E, M, N]) VertexMailbox(internalOrOffset uint32) *VertexMailbox[M] {
	idx := internalOrOffset & THREAD_MASK
	bucket, bpos := idxToBucket(idx)
	return &gt.VertexMailboxes[bucket][bpos]
}

// Wrapper for getting a vertex structure; okay to provide a vidx or just the thread-local offset.
func (gt *GraphThread[V, E, M, N]) VertexStructure(internalOrOffset uint32) *VertexStructure {
	bucket, bpos := idxToBucket(internalOrOffset & THREAD_MASK)
	return &gt.VertexStructures[bucket][bpos]
}

// Wrapper for getting a vertex raw ID; okay to provide a vidx or just the thread-local offset.
func (gt *GraphThread[V, E, M, N]) VertexRawID(internalOrOffset uint32) RawType {
	bucket, bpos := idxToBucket(internalOrOffset & THREAD_MASK)
	return gt.VertexStructures[bucket][bpos].RawId
}

// ------------------ Node level functions ------------------ //

// Node level, vertex reference from Raw ID.
func (g *Graph[V, E, M, N]) NodeVertexFromRaw(rawId RawType) (uint32, *Vertex[V, E]) {
	if internalId, ok := g.GraphThreads[rawId.Within(g.NumThreads)].VertexMap[rawId]; ok {
		return internalId, g.NodeVertex(internalId)
	}
	return 0, nil
}

// Node level, vertex reference from internal index.
func (g *Graph[V, E, M, N]) NodeVertex(internalId uint32) *Vertex[V, E] {
	id, tidx := InternalExpand(internalId)
	return &g.GraphThreads[tidx].Vertices[id]
}

func (g *Graph[V, E, M, N]) NodeVertexProperty(internalId uint32) *V {
	id, tidx := InternalExpand(internalId)
	bucket, bpos := idxToBucket(id)
	return &g.GraphThreads[tidx].VertexProperties[bucket][bpos]
}

// Node level, vertex reference from internal index. Returns nil of the vertex does not exist.
func (g *Graph[V, E, M, N]) NodeVertexOrNil(internalId uint32) *Vertex[V, E] {
	id, tidx := InternalExpand(internalId)
	if int(id) >= len(g.GraphThreads[tidx].Vertices) {
		return nil
	}
	return &g.GraphThreads[tidx].Vertices[id]
}

// Node level, gives a reference to the vertex mailbox, and the thread index.
func (g *Graph[V, E, M, N]) NodeVertexMailbox(internalId uint32) (*VertexMailbox[M], uint32) {
	idx, tidx := InternalExpand(internalId)
	bucket, bpos := idxToBucket(idx)
	return &g.GraphThreads[tidx].VertexMailboxes[bucket][bpos], tidx
}

// Node level, retrieves the supplemental structure for a vertex from a given internal index.
func (g *Graph[V, E, M, N]) NodeVertexStructure(internalId uint32) *VertexStructure {
	idx, tidx := InternalExpand(internalId)
	return g.GraphThreads[tidx].VertexStructure(idx)
}

// Node level, retrieves the raw id for a vertex from a given internal index.
func (g *Graph[V, E, M, N]) NodeVertexRawID(internalId uint32) RawType {
	idx, tidx := InternalExpand(internalId)
	return g.GraphThreads[tidx].VertexRawID(idx)
}

// Node level, retrieves the in-event position for a vertex from a given internal index.
func (g *Graph[V, E, M, N]) NodeVertexInEventPos(internalId uint32) uint32 {
	idx, tidx := InternalExpand(internalId)
	return g.GraphThreads[tidx].VertexStructure(idx).InEventPos
}

// Node level, not thread safe. Should only be used outside async processing, or for debugging.
func (g *Graph[V, E, M, N]) NodeVertexCount() int {
	sum := 0
	for t := 0; t < int(g.NumThreads); t++ {
		sum += len(g.GraphThreads[t].Vertices)
	}
	return sum
}

// Node level, basic iteration over all vertices in the graph.
// Gives an applicator an ordinal index i [0, |V|), the internal index, and a pointer, to the vertex.
func (g *Graph[V, E, M, N]) NodeForEachVertex(applicator func(ordinal uint32, internalId uint32, vertex *Vertex[V, E], prop *V)) {
	count := uint32(0)
	for tidx := uint32(0); tidx < g.NumThreads; tidx++ {
		gt := &g.GraphThreads[tidx]
		threadOffset := (tidx << THREAD_SHIFT)
		for i := uint32(0); i < uint32(len(gt.Vertices)); i++ {
			bucket, bpos := idxToBucket(i)
			applicator(count, (threadOffset | i), &gt.Vertices[i], &gt.VertexProperties[bucket][bpos])
			count++
		}
	}
}

// Node level, performs an applicator function on each graph thread. All threads run in parallel.
// Consider i to be in range [0, gt.Vertices ) for the graph thread gt.
// The ordinalStart is a start ordinal offset for the graph thread, in range [0, g.NodeVertexCount ).
// May be useful if you wish to write to an external array of size NodeVertexCount, in which case you can use (ordinalStart + i) as the index.
// The threadOffset is used to calculate the internal index, from the given graph thread's ordinal index of [0, len(gt.Vertices)),
// use (threadOffset | i) to get the internal index used within the system.
// Sums the return values of the applicator function.
func (g *Graph[V, E, M, N]) NodeParallelFor(applicator func(ordinalStart uint32, threadOffset uint32, gt *GraphThread[V, E, M, N]) (accumulated int)) (accumulator int) {
	res := make(chan int, g.NumThreads)
	ordinalStart := uint32(0)
	for t := uint32(0); t < g.NumThreads; t++ {
		go func(tidx uint32, ordinalStart uint32, gt *GraphThread[V, E, M, N]) {
			tAcc := 0
			tAcc += applicator(ordinalStart, (tidx << THREAD_SHIFT), gt)
			res <- tAcc
		}(t, ordinalStart, &g.GraphThreads[t])
		ordinalStart += uint32(len(g.GraphThreads[t].Vertices))
	}
	for t := uint32(0); t < g.NumThreads; t++ {
		accumulator += <-res
	}
	return accumulator
}

// For oracle comparison, newly allocates mailbox buckets.
func (gt *GraphThread[V, E, M, N]) NodeCopyVertexMailboxes() (out []*[BUCKET_SIZE]VertexMailbox[M]) {
	out = make([]*[BUCKET_SIZE]VertexMailbox[M], len(gt.VertexMailboxes))
	for b := 0; b < len(gt.VertexMailboxes); b++ {
		out[b] = new([BUCKET_SIZE]VertexMailbox[M])
	}
	return out
}

// For oracle comparison, copies vertices array. Tries to reuse the other array if possible.
func (gt *GraphThread[V, E, M, N]) NodeCopyVerticesInto(other *[]Vertex[V, E]) {
	diff := len(gt.Vertices) - len(*other)
	if diff > 0 { // Other may be smaller, check if we must expand.
		(*other) = append((*other), make([]Vertex[V, E], diff)...)
	}
	copy(*other, gt.Vertices)
}

// For oracle comparison, copies vertex properties.
func (gt *GraphThread[V, E, M, N]) NodeCopyVertexPropsInto(other *[]*[BUCKET_SIZE]V) {
	diff := len(gt.VertexProperties) - len(*other)
	if diff > 0 { // Other may be smaller, check if we must expand.
		(*other) = append((*other), make([]*[BUCKET_SIZE]V, diff)...)

	}
	for b := 0; b < len(gt.VertexProperties); b++ {
		if (*other)[b] == nil {
			(*other)[b] = new([BUCKET_SIZE]V)
		}
		copy((*other)[b][:], gt.VertexProperties[b][:])
	}
}

// Copies the size of the vertex property bucket to the other. Does not copy the buckets themselves.
func (gt *GraphThread[V, E, M, N]) NodeAllocatePropertyBuckets(other *[]*[BUCKET_SIZE]V) {
	diff := len(gt.VertexProperties) - len(*other)
	if diff > 0 { // Check if we must expand.
		(*other) = append((*other), make([]*[BUCKET_SIZE]V, diff)...)
		// allocate new buckets
		for i := 0; i < diff; i++ {
			(*other)[len(*other)-diff+i] = new([BUCKET_SIZE]V)
		}
	}
}
