package main

import (
	"math"

	"github.com/ScottSallinen/lollipop/graph"
)

type Template struct{}

// Defines when a message is deemed empty, uninitialized, etc.
const EMPTY_VAL = math.MaxFloat64

// Defines the properties stored per vertex. Can be used below within the algorithm.
type VertexProperty struct {
	// Example, a value for a vertex.
	Value float64
}

// Defines the properties stored per edge.
type EdgeProperty struct {
	// The graph edge type is what will be parsed from the input stream. Check edge.go for more info.
	graph.EmptyEdge // Inherit a default defined in edge.go, or define your own.
	// Attach algorithm properties here if you like.
}

// Defines the properties stored per message.
type Message struct {
	Value float64
}

type Note struct{}

// Default constructor for a vertex property. Called when a new vertex is added to the graph (it has no edges yet).
// Useful if the zero value is not meant to be 0.
func (VertexProperty) New() (new VertexProperty) {
	// example: new.Value = math.MaxFloat64
	return new
}

// Default constructor for a message. Called when a new message is created.
// Useful if the zero value is not meant to be 0.
func (Message) New() (new Message) {
	// example: return Message{math.MaxFloat64}
	return Message{EMPTY_VAL}
}

// Defines an initial message that will be sent to all vertices.
// It will have a unique property that the recipient sees the sender as itself.
// Note that this message goes through the regular flow; it is merged, checked for new info, and then the vertex may be retrieve->updated.
// Because of this, note that it is not guaranteed that this message will be the first message a vertex sees.
// If more advanced requirements beyond the default constructor and this message is necessary, use the BaseVertexMessage below.
func (*Template) InitAllMessage(vertex *graph.Vertex[VertexProperty, EdgeProperty], internalId uint32, rawId graph.RawType) Message {
	return Message{EMPTY_VAL}
}

// Optional to declare.
// If declared, will run the first time for a vertex when it is initiated for an algorithm.
// Note that this has unique semantics; while the default constructors above are always called on vertex creation,
// which is the same for dynamic and static algorithms, this is called for dynamic algorithms in right after the creation
// (so the vertex will have no edges, as with the default constructor), but for static algorithms, this is called
// after the graph is built, and the vertex will have edges, and the view into the vertex could be useful.
// This is generally not a good idea to declare if it can be avoided, as it requires a full loop over vertices,
// before any real algorithmic processing occurs for static graphs.
// The returned message will overwrite the default constructor message for that vertex.
func (*Template) BaseVertexMessage(vertex *graph.Vertex[VertexProperty, EdgeProperty], internalId uint32, rawId graph.RawType) Message {
	return Message{EMPTY_VAL}
}

// When multiple messages are for a vertex, how should we aggregate the info?
// The sender (sidx) is putting an incoming message into the existing message.
// This is called by the sender, targeting the message inbox of the existing vertex.
// Should return true if the outcome has new information, and the target vertex should be notified.
// This needs to be thread safe.
func (*Template) MessageMerge(incoming Message, sidx uint32, existing *Message) (newInfo bool) {
	// example: utils.AtomicMinFloat64(&existing.Value, incoming.Value)
	return false
}

// When we need the message data. This is called before an update to a vertex.
// The view into the vertex here presents a small opportunity to optimize, as this is receiver side aggregation.
// This needs to be thread safe.
func (*Template) MessageRetrieve(existing *Message, vertex *graph.Vertex[VertexProperty, EdgeProperty]) (outgoing Message) {
	// example: outgoing.Value = utils.AtomicLoadFloat64(&existing.Value)
	return outgoing
}

// The main function for basic algorithm behaviour, and is the entry point.
// The data is pulled using aggregate retrieve above before being handed to this function.
// Note that there is not guaranteed to by any actual message, depending on the result of the retrieve.
// Return the number of messages you sent.
func (*Template) OnUpdateVertex(g *graph.Graph[VertexProperty, EdgeProperty, Message, Note], src *graph.Vertex[VertexProperty, EdgeProperty], n graph.Notification[Note], m Message) (sent uint64) {
	return 0
}

// Function called upon new edge(s) added to the vertex src. This also bundles a visit, including any new messages, from MessageRetrieve for this vertex.
// Ensure you handle these messages (it may be just calling OnUpdateVertex with the message, but consider merging if possible -- e.g. you may message all vertices anyway).
// The view here is **post** addition (the edges are already appended to the edge list).
// Note: eidxStart is the first position of new edges in the src.OutEdges array. (Also note OutEdges may contain multiple edges with the same destination.)
func (*Template) OnEdgeAdd(g *graph.Graph[VertexProperty, EdgeProperty, Message, Note], src *graph.Vertex[VertexProperty, EdgeProperty], sidx uint32, eidxStart int, m Message) (sent uint64) {
	return 0
}

// This function is to be called with a set of edge deletion events.
func (*Template) OnEdgeDel(g *graph.Graph[VertexProperty, EdgeProperty, Message, Note], src *graph.Vertex[VertexProperty, EdgeProperty], sidx uint32, deletedEdges []graph.Edge[EdgeProperty], m Message) (sent uint64) {
	return 0
}

// Optional to declare.
// If declared, this function is called after the processing is complete; in case any finalization step (e.g. normalization) is needed.
// Otherwise, no need to declare.
func (*Template) OnFinish(g *graph.Graph[VertexProperty, EdgeProperty, Message, Note]) {

}
