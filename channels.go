package raft

type channels struct {
	command       chan *commandEvent
	voteRequest   chan *voteRequestEvent
	appendRequest chan *appendRequestEvent

	voteResponse   chan *RequestVoteResponse
	appendResponse chan *AppendEntriesResponse
	stop           chan chan bool
}

type commandEvent struct {
	command Command
	err     error
	result  chan interface{}
}

type voteRequestEvent struct {
	request  *RequestVoteRequest
	response chan *RequestVoteResponse
}

type appendRequestEvent struct {
	request  *AppendEntriesRequest
	response chan *AppendEntriesResponse
}

func newChannels() *channels {
	return &channels{
		stop:           make(chan chan bool, 1),
		command:        make(chan *commandEvent, 256),
		voteRequest:    make(chan *voteRequestEvent),
		appendRequest:  make(chan *appendRequestEvent),
		voteResponse:   make(chan *RequestVoteResponse, 16), // size should be larger than the number of peers
		appendResponse: make(chan *AppendEntriesResponse, 16),
	}
}

func (c *channels) sendCommand(command Command) (interface{}, error) {
	e := &commandEvent{
		command: command,
		result:  make(chan interface{}, 1),
	}
	c.command <- e
	result := <-e.result
	return result, e.err
}

func (c *channels) sendAppendRequest(a *AppendEntriesRequest) *AppendEntriesResponse {
	ae := &appendRequestEvent{
		request:  a,
		response: make(chan *AppendEntriesResponse, 1),
	}
	c.appendRequest <- ae
	return <-ae.response
}

func (c *channels) sendAppendResponse(ar *AppendEntriesResponse) {
	select {
	case c.appendResponse <- ar:
	default:
		panic("cannot.send.append.response.")
	}
}

func (c *channels) sendVoteRequest(v *RequestVoteRequest) *RequestVoteResponse {
	ve := &voteRequestEvent{
		request:  v,
		response: make(chan *RequestVoteResponse, 1),
	}
	c.voteRequest <- ve

	return <-ve.response
}

func (c *channels) sendVoteResponse(vr *RequestVoteResponse) {
	select {
	case c.voteResponse <- vr:
	default:
		panic("cannot.send.vote.response.")
	}
}

func newCommandEvent(c Command) *commandEvent {
	return &commandEvent{
		command: c,
		result:  make(chan interface{}, 1),
	}
}
