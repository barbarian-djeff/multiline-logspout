package logstash

import (
	"encoding/json"
	"errors"
	_ "expvar"
	"net"
	"regexp"
	"strconv"
	"time"

	"github.com/barbarian-djeff/multiline-logspout/multiline"

	"log"
	"os"

	"github.com/gliderlabs/logspout/router"
	"github.com/rcrowley/go-metrics"
	"github.com/rcrowley/go-metrics/exp"
	"strings"
)

var (
	logMeter = metrics.NewMeter()
)

func init() {
	router.AdapterFactories.Register(NewLogstashAdapter, "multiline")
	exp.Exp(metrics.DefaultRegistry)
	metrics.Register("logstash_message_rate", logMeter)
}

type newMultilineBufferFn func() (multiline.MultiLine, error)

// LogstashAdapter is an adapter that streams TCP JSON to Logstash.
type LogstashAdapter struct {
	write            writer
	route            *router.Route
	cache            map[string]*multiline.MultiLine
	cacheTTL         time.Duration
	cachedLines      metrics.Gauge
	mkBuffer         newMultilineBufferFn
	cleanupRegExp    *regexp.Regexp
	doNotSendJustLog bool
}

type ControlCode int

const (
	Continue ControlCode = iota
	Quit

	// pattern used to detect any line part of a stack trace
	// group 1: empty line
	// group 2: start with '   at'
	// group 3: start with 'Caused by'
	// group 4: start with the path of a java class followed by ':'
	// group 5: start with '   ...'
	IsMultilineDefaultPattern = `(^\s*$)|(^\s+at)|(^Caused by:)|(^[a-z]+[a-zA-Z0-9\.$_]+:\s)|(^\s+\.{3})`
)

func newLogstashAdapter(route *router.Route, write writer) *LogstashAdapter {
	log.Printf("# create multiline adapter with options %s\n", route.Options)

	patternString, ok := route.Options["pattern"]
	if !ok {
		patternString = IsMultilineDefaultPattern
	}

	groupWith, ok := route.Options["group_with"]
	if !ok {
		groupWith = "previous"
	}

	negate := false
	negateStr, _ := route.Options["negate"]
	if negateStr == "true" {
		negate = true
	}

	separator, ok := route.Options["separator"]
	if !ok {
		separator = "\n"
	}

	maxLines, err := strconv.Atoi(route.Options["max_lines"])
	if err != nil {
		maxLines = 0
	}

	cacheTTL, err := time.ParseDuration(route.Options["cache_ttl"])
	if err != nil {
		cacheTTL = 10 * time.Second
	}

	cleanupPattern, ok := route.Options["cleanup_pattern"]
	if !ok {
		cleanupPattern = `\033\[[0-9;]*?m`
	}

	cleanupRegExp := regexp.MustCompile(cleanupPattern)

	cachedLines := metrics.NewGauge()
	metrics.Register(route.ID+"_cached_lines", cachedLines)

	doNotSendJustLog := os.Getenv("DO_NOT_SEND_JUST_LOG") == "true"

	return &LogstashAdapter{
		route:       route,
		write:       write,
		cache:       make(map[string]*multiline.MultiLine),
		cacheTTL:    cacheTTL,
		cachedLines: cachedLines,
		mkBuffer: func() (multiline.MultiLine, error) {
			return multiline.NewMultiLine(
				&multiline.MultilineConfig{
					Pattern:   regexp.MustCompile(patternString),
					GroupWith: groupWith,
					Negate:    negate,
					Separator: &separator,
					MaxLines:  maxLines,
				})
		},
		cleanupRegExp:    cleanupRegExp,
		doNotSendJustLog: doNotSendJustLog,
	}
}

// NewLogstashAdapter creates a LogstashAdapter with TCP as the default transport.
func NewLogstashAdapter(route *router.Route) (router.LogAdapter, error) {
	transportId, ok := route.Options["transport"]
	if !ok {
		transportId = "tcp"
	}

	transport, found := router.AdapterTransports.Lookup(route.AdapterTransport(transportId))
	if !found {
		return nil, errors.New("unable to find adapter: " + route.Adapter)
	}

	conn, err := transport.Dial(route.Address, route.Options)
	if err != nil {
		return nil, err
	}

	var write writer
	if transportId == "tcp" {
		write = tcpWriter(conn)
	} else {
		write = defaultWriter(conn)
	}

	return newLogstashAdapter(route, write), nil
}

func (a *LogstashAdapter) lookupBuffer(msg *router.Message) *multiline.MultiLine {
	key := msg.Container.ID + msg.Source
	if a.cache[key] == nil {
		ml, _ := a.mkBuffer()
		a.cache[key] = &ml
	}
	return a.cache[key]
}

// Stream implements the router.LogAdapter interface.
func (a *LogstashAdapter) Stream(logstream chan *router.Message) {
	cacheTicker := time.NewTicker(a.cacheTTL).C

	for {
		msgs, ccode := a.readMessages(logstream, cacheTicker)
		a.sendMessages(msgs)

		switch ccode {
		case Continue:
			continue
		case Quit:
			return
		}
	}
}

func (a *LogstashAdapter) readMessages(
	logstream chan *router.Message,
	cacheTicker <-chan time.Time) ([]*router.Message, ControlCode) {
	select {
	case t := <-cacheTicker:
		return a.expireCache(t), Continue
	case msg, ok := <-logstream:
		if ok {
			return a.bufferMessage(msg), Continue
		} else {
			return a.flushPendingMessages(), Quit
		}
	}
}

func (a *LogstashAdapter) bufferMessage(msg *router.Message) []*router.Message {
	msgOrNil := a.lookupBuffer(msg).Buffer(msg)

	if msgOrNil == nil {
		return []*router.Message{}
	} else {
		return []*router.Message{msgOrNil}
	}
}

func (a *LogstashAdapter) expireCache(t time.Time) []*router.Message {
	var messages []*router.Message
	var linesCounter int64 = 0

	for id, buf := range a.cache {
		linesCounter += int64(buf.PendingSize())
		msg := buf.Expire(t, a.cacheTTL)
		if msg != nil {
			messages = append(messages, msg)
			delete(a.cache, id)
		}
	}

	a.cachedLines.Update(linesCounter)

	return messages
}

func (a *LogstashAdapter) flushPendingMessages() []*router.Message {
	var messages []*router.Message

	for _, buf := range a.cache {
		msg := buf.Flush()
		if msg != nil {
			messages = append(messages, msg)
		}
	}

	return messages
}

func (a *LogstashAdapter) sendMessages(msgs []*router.Message) {
	for _, msg := range msgs {
		if err := a.sendMessage(msg); err != nil {
			log.Fatal("logstash:", err)
		}
	}
	logMeter.Mark(int64(len(msgs)))
}

func (a *LogstashAdapter) sendMessage(msg *router.Message) error {
	buff, err := a.serialize(msg)
	if err != nil {
		return err
	}

	if a.doNotSendJustLog {
		log.Printf("buffer is not sent to logstash: %s\n", string(buff))
	} else {
		_, err = a.write(buff)
		if err != nil {
			return err
		}
	}

	return nil
}

func (a *LogstashAdapter) serialize(msg *router.Message) ([]byte, error) {
	var js []byte
	var jsonMsg map[string]interface{}

	dockerInfo := DockerInfo{
		Name:     msg.Container.Name,
		ID:       msg.Container.ID,
		Image:    msg.Container.Config.Image,
		Hostname: msg.Container.Config.Hostname,
	}

	dockerInfo.Labels = make(map[string]string)
	for label, value := range msg.Container.Config.Labels {
		dockerInfo.Labels[strings.Replace(label, ".", "_", -1)] = value
	}

	err := json.Unmarshal([]byte(msg.Data), &jsonMsg)
	if err != nil {
		// the message is not in JSON make a new JSON message
		msgToSend := LogstashMessage{
			Message:   msg.Data,
			Docker:    dockerInfo,
			Stream:    msg.Source,
		}
		js, err = json.Marshal(msgToSend)
		if err != nil {
			return nil, err
		}
	} else {
		// the message is already in JSON just add the docker specific fields as a nested structure
		jsonMsg["docker"] = dockerInfo
		js, err = json.Marshal(jsonMsg)
		if err != nil {
			return nil, err
		}
	}

	return js, nil
}

type DockerInfo struct {
	Name     string            `json:"name"`
	ID       string            `json:"id"`
	Image    string            `json:"image"`
	Hostname string            `json:"hostname"`
	Labels   map[string]string `json:"labels"`
}

type ComponentInfo struct {
	Name    string `json:"name"`
	Env     string `json:"env"`
	Version string `json:"version"`
}

type LogstashMessage struct {
	Message   string        `json:"message"`
	Stream    string        `json:"stream"`
	Docker    DockerInfo    `json:"docker"`
}

// writers
type writer func(b []byte) (int, error)

func defaultWriter(conn net.Conn) writer {
	return func(b []byte) (int, error) {
		return conn.Write(b)
	}
}

func tcpWriter(conn net.Conn) writer {
	return func(b []byte) (int, error) {
		// append a newline
		return conn.Write([]byte(string(b) + "\n"))
	}
}
