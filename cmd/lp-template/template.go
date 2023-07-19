package main

import (
	"math"

	"github.com/ScottSallinen/lollipop/graph"
)

type Template struct{}

// Defines when mail is deemed empty, uninitialized, etc.
const EMPTY_VAL = math.MaxUint64

// Defines the properties stored per vertex. Can be used below within the algorithm.
type VertexProperty struct {
	// Example, a value for a vertex.
	Value uint64
}

// Defines the properties stored per edge.
type EdgeProperty struct {
	// The graph edge type is what will be parsed from the input stream. Check graph-edge.go for more info.
	graph.EmptyEdge // Inherit a default defined in edge.go, or define your own.
	// Attach algorithm properties here if you like.
}

// Defines the properties stored per mail.
type Mail struct {
	Value uint64
}

// Defines additional properties, if any, sent along with a notification.
// A notification is what marks a vertex as a target for processing an update cycle. It implies the mailbox, or the notification itself, contains information that is likely useful to process.
// When using unique notifications, only the property of the first notification would exist (subsequent notifications are discarded) -- hence, attaching algorithm values here is likely not useful.
// When using active notifications, notifications are enqueued and thus attached values to this queue becomes useful, as it enables a "pure-message-passing" strategy.
// Importantly, "sync" emulation strategys are only compatible with unique notifications -- and the strategy also assumes the notification has no attached algorithmic property.
// Async strategies can use either. (Or perhaps even a mixture, but thats not recommended as the behaviour may not be well defined).
type Note struct{}

// "Default constructor" for a vertex property. Called when a new vertex is added to the graph (it has no edges yet).
// Useful if the zero value is not meant to be 0.
func (VertexProperty) New() (new VertexProperty) {
	// example: new.Value = math.MaxFloat64
	return new
}

// "Default constructor" for the base value mail in a mailbox. Called when a mailbox is created (when a vertex is newly referenced).
// Useful if the zero value is not meant to be 0.
func (Mail) New() (new Mail) {
	// example: return Mail{math.MaxFloat64}
	return Mail{EMPTY_VAL}
}

// Optional to declare; either use this or initMail.
// Defines a initial mail that will be sent to all vertices.
// It will have a unique property that the recipient sees the sender as itself.
// Note that this mail goes through a regular flow; it is merged into a mailbox, checked for new info, and then the vertex can retrieve mail then update.
// Because of this, note that it is not guaranteed that this mail will be the first information a vertex sees.
// If more advanced requirements beyond the default constructor and this mail is necessary, use the BaseVertexMailbox below.
func (*Template) InitAllMail(vertex *graph.Vertex[VertexProperty, EdgeProperty], internalId uint32, rawId graph.RawType) Mail {
	return Mail{EMPTY_VAL}
}

// Optional to declare.
// If declared, will run the first time for a vertex when it is initiated for an algorithm.
// Note that this has unique semantics; while the default constructors above are always called on vertex creation,
// which is the same for dynamic and static algorithms, this is called for dynamic algorithms in right after the creation
// (so the vertex will have no edges, as with the default constructor), but for static algorithms, this is called
// after the graph is built, and the vertex will have edges, and the view into the vertex could be useful.
// This is generally not a good idea to declare if it can be avoided, as it requires a full loop over vertices,
// before any real algorithmic processing occurs for static graphs.
// The returned mail will overwrite the "default constructor" mailbox for that vertex.
func (*Template) BaseVertexMailbox(vertex *graph.Vertex[VertexProperty, EdgeProperty], internalId uint32, s *graph.VertexStructure) Mail {
	return Mail{EMPTY_VAL}
}

// When multiple mail are for a vertex, how should we aggregate the info?
// The sender "sidx" is putting an "incoming" mail into the "existing" mailbox.
// This is **sender side** aggregation, called by the sender, targeting the inbox of the existing vertex.
// Should return true if the outcome has new information, and the target vertex should be notified.
// This needs to be thread safe.
func (*Template) MailMerge(incoming Mail, sidx uint32, existing *Mail) (newInfo bool) {
	// example: utils.AtomicMinFloat64(&existing.Value, incoming.Value)
	return false
}

// When we need the mailbox data (existing). This is called imediately before an update to a vertex, and this is **receiver side**.
// The view into the vertex here also presents an opportunity to optimize, e.g., move data into the vertex property.
// The outgoing mail here is what is sent to the OnUpdateVertex immediately after.
// This needs to be thread safe.
func (*Template) MailRetrieve(existing *Mail, vertex *graph.Vertex[VertexProperty, EdgeProperty]) (outgoing Mail) {
	// example: outgoing.Value = utils.AtomicLoadFloat64(&existing.Value)
	return outgoing
}

// The main function for basic algorithm behaviour, and is the entry point.
// The data is pulled using aggregate retrieve above before being handed to this function.
// Note that there is not guaranteed to be any actual useful information, depending on the result of the retrieve.
// Return the number of messages you sent.
func (alg *Template) OnUpdateVertex(g *graph.Graph[VertexProperty, EdgeProperty, Mail, Note], src *graph.Vertex[VertexProperty, EdgeProperty], n graph.Notification[Note], m Mail) (sent uint64) {
	/*
		// Example: send mail to all neighbours. Use the unique notification strategy.
		// The mail is algorithm defined. Here, a simple example is sending our outgoing edge length to all our neighbours.
		mail := Mail{uint64(len(src.OutEdges))}
		for _, e := range src.OutEdges {
			// Get the mailbox for the target vertex. (The destination graph thread identity will be needed for the framework as well.)
			mailbox, tidx := g.NodeVertexMailbox(e.Didx)
			// Merge the mail (1st arg), sent by ourself (2nd), into the targets mailbox (3rd). If this returns true, this means the target vertex should be notified.
			// The identity of "ourself": as we were instructed to run our update cycle due to a notification, "ourself" is known by that input argument (n.Target).
			if alg.MailMerge(mail, n.Target, &mailbox.Inbox) {
				// The merge returned true, so we should send a notification to the target.
				// In this example we use a unique notification to the target (edge). Note again that the sender (1st arg) is "ourself".
				sent += g.EnsureSend(g.UniqueNotification(n.Target, graph.Notification[Note]{Target: e.Didx}, mailbox, tidx))
			}
		}
	*/
	// As a contract to the framework design, the update function must return the number of notifications (not mail!) that were sent.
	// Note a unique notification may not actually send (e.g. target already active), so use the return value, and do not assume.
	return sent
}

// Function called upon new edge(s) added to the vertex src. This also bundles a visit, including any new mail, from MailRetrieve for this vertex.
// Ensure you handle this mail (it may be just calling OnUpdateVertex with the mail, but consider merging if possible -- e.g. you may target all vertices anyway).
// The view here is **post** addition (the edges are already appended to the edge list).
// Note: eidxStart is the first position of new edges in the src.OutEdges array. (Also note OutEdges may contain multiple edges with the same destination.)
func (*Template) OnEdgeAdd(g *graph.Graph[VertexProperty, EdgeProperty, Mail, Note], src *graph.Vertex[VertexProperty, EdgeProperty], sidx uint32, eidxStart int, m Mail) (sent uint64) {
	return 0
}

// This function is to be called with a set of edge deletion events.
func (*Template) OnEdgeDel(g *graph.Graph[VertexProperty, EdgeProperty, Mail, Note], src *graph.Vertex[VertexProperty, EdgeProperty], sidx uint32, deletedEdges []graph.Edge[EdgeProperty], m Mail) (sent uint64) {
	return 0
}

// Optional to declare.
// If declared, this function is called after the processing is complete; in case any finalization step (e.g. normalization) is needed.
// Otherwise, no need to declare.
func (*Template) OnFinish(g *graph.Graph[VertexProperty, EdgeProperty, Mail, Note]) {

}
