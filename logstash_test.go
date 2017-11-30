package logstash

import (
	"encoding/json"
	"net"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/gliderlabs/logspout/router"
	_ "github.com/gliderlabs/logspout/transports/tcp"
	_ "github.com/gliderlabs/logspout/transports/udp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/fsouza/go-dockerclient"
)

func makeMockWriter() (writer, *[]string) {
	var tmp []string
	results := &tmp
	return func(b []byte) (int, error) {
		*results = append(*results, string(b))
		return 0, nil
	}, results
}

func TestMultilineDetectionPattern(t *testing.T) {
	isMultiline := regexp.MustCompile(IsMultilineDefaultPattern)

	notMultilines := []string{
		"2017-11-08 10:34:08.021  WARN [account-service,,,] 1 --- [nfoReplicator-0] c.n.d.s.t.d.RetryableEurekaHttpClient    : Request execution failed with message: java.net.UnknownHostException: eureka-server",
		"2017-11-08 10:34:08.023  WARN [account-service,,,] 1 --- [nfoReplicator-0] com.netflix.discovery.DiscoveryClient    : DiscoveryClient_ACCOUNT-SERVICE/3a60280e0b97:account-service:8091 - registration failed Cannot execute request on any known server",
		"2017-11-08 10:34:08.024  WARN [account-service,,,] 1 --- [nfoReplicator-0] c.n.discovery.InstanceInfoReplicator     : There was a problem with the instance info replicator",
		"SLF4J: See http://www.slf4j.org/codes.html#multiple_bindings for an explanation.  ",
	}
	for _, line := range notMultilines {
		require.False(t, isMultiline.MatchString(line))
	}

	multilines := []string{
		"com.netflix.discovery.shared.transport.TransportException: Cannot execute request on any known server",
		"",
		"\n",
		"  ",
		"	",
		"	at com.netflix.discovery.shared.transport.decorator.RetryableEurekaHttpClient.execute(RetryableEurekaHttpClient.java:111) ~[eureka-client-1.6.2.jar!/:1.6.2]",
		"Caused by: java.net.UnknownHostException: eureka-server",
		"	... 29 common frames omitted",
	}
	for _, line := range multilines {
		require.True(t, isMultiline.MatchString(line), "this line should be part of a multiline: "+line)
	}
}

func TestStreamMultiline(t *testing.T) {
	assert := assert.New(t)

	mockWriter, results := makeMockWriter()
	adapter := newLogstashAdapter(new(router.Route), mockWriter)

	assert.NotNil(adapter)

	logstream := make(chan *router.Message)
	container := makeDummyContainer("anid")
	lines := []string{
		"Line1",
		"   Line1.1",
	}

	go pump(logstream, &container, [][]string{lines})

	adapter.Stream(logstream)
	data := parseResult(assert, (*results)[0])

	assert.Equal("Line1", data["message"])
	assertDockerInfo(assert, &container, data["docker"])
}

func TestStreamMultilineStacktrace(t *testing.T) {
	assert := assert.New(t)

	mockWriter, results := makeMockWriter()
	adapter := newLogstashAdapter(new(router.Route), mockWriter)

	assert.NotNil(adapter)

	logstream := make(chan *router.Message)
	container := makeDummyContainer("anid")
	lines := []string{
		"2017-11-08 10:34:08.023  WARN [account-service,,,] 1 --- [nfoReplicator-0] com.netflix.discovery.DiscoveryClient    : DiscoveryClient_ACCOUNT-SERVICE/3a60280e0b97:account-service:8091 - registration failed Cannot execute request on any known server",
		"	at org.eclipse.jetty.util.thread.QueuedThreadPool$3.run(QueuedThreadPool.java:572) [jetty-util-9.3.0.v20150612.jar:9.3.0.v20150612]",
		"	at java.lang.Thread.run(Thread.java:745) [?:1.8.0_25]",
		"	at com.mm.first.ge.controller.BlackListController.blackListSync(BlackListController.java:26) ~[main/:?]",
		"Caused by: java.lang.IllegalArgumentException: Message test",
		"	at com.mm.blacklist.ge.controller.BlackListController.blackListSync(BlackListController.java:26) ~[main/:?]",
		"	at sun.reflect.NativeMethodAccessorImpl.invoke0(Native Method) ~[?:1.8.0_25]",
		"no_timestamp INFO [account-service,,,] 1 --- [nfoReplicator-0] com.netflix.discovery.DiscoveryClient    : DiscoveryClient_ACCOUNT-SERVICE/3a60280e0b97:account-service:8091 - registration failed Cannot execute request on any known server",
	}

	go pump(logstream, &container, [][]string{lines})

	adapter.Stream(logstream)
	data := parseResult(assert, (*results)[0])

	assert.Equal(strings.Join(lines[0:7], "\n"), data["message"])
	assert.Equal("FOOOOO", data["stream"])
	assert.Equal("2017-11-08 10:34:08.023", data["message"].(string)[0:23])
	assertDockerInfo(assert, &container, data["docker"])

	data = parseResult(assert, (*results)[1])
	assert.Equal(lines[7], data["message"])
}

func TestStreamJson(t *testing.T) {
	assert := assert.New(t)
	mockWriter, results := makeMockWriter()
	adapter := newLogstashAdapter(new(router.Route), mockWriter)
	assert.NotNil(adapter)
	logstream := make(chan *router.Message)
	container := makeDummyContainer("anid")

	rawLine := `{ "remote_user": "-",
                "body_bytes_sent": "25",
                "request_time": "0.821",
                "status": "200",
                "request_method": "POST",
                "http_referrer": "-",
                "http_user_agent": "-" }`

	go pump(logstream, &container, [][]string{{rawLine}})

	adapter.Stream(logstream)
	data := parseResult(assert, (*results)[0])

	assert.Equal("-", data["remote_user"])
	assert.Equal("25", data["body_bytes_sent"])
	assert.Equal("0.821", data["request_time"])
	assert.Equal("200", data["status"])
	assert.Equal("POST", data["request_method"])
	assert.Equal("-", data["http_referrer"])
	assert.Equal("-", data["http_user_agent"])
	assert.Equal(nil, data["java_timestamp"])
	assert.Equal(nil, data["log_level"])

	assertDockerInfo(assert, &container, data["docker"])
}

func TestStreamMultipleMixedMessages(t *testing.T) {
	assert := assert.New(t)

	mockWriter, results := makeMockWriter()
	adapter := newLogstashAdapter(new(router.Route), mockWriter)

	logstream := make(chan *router.Message)
	container := makeDummyContainer("anid")
	expected := [][]string{
		{
			"Line1",
			"   Line1.1",
		},
		{
			`{"message":"I am json"}`,
		},
	}

	go pump(logstream, &container, expected)

	adapter.Stream(logstream)

	// first line
	data := parseResult(assert, (*results)[0])
	assert.Equal("Line1", data["message"])

	// second line
	data = parseResult(assert, (*results)[1])
	assert.Equal("   Line1.1", data["message"])

	// second message
	data = parseResult(assert, (*results)[2])
	assert.Equal("I am json", data["message"])
}

func TestCacheExpiration(t *testing.T) {
	assert := assert.New(t)

	mockWriter, results := makeMockWriter()
	var r router.Route
	r.Options = make(map[string]string)
	r.Options["cache_ttl"] = "5ms"
	adapter := newLogstashAdapter(&r, mockWriter)
	logstream := make(chan *router.Message)
	container := makeDummyContainer("anid")

	go func() {
		msg := makeDummyMessage(&container, "test")
		logstream <- &msg
	}()

	go adapter.Stream(logstream)

	time.Sleep(15 * time.Millisecond)

	assert.Equal(1, len(*results), "cache timer must fire to force message flush")
	data := parseResult(assert, (*results)[0])
	assert.Equal("test", data["message"])

	close(logstream)
}

func TestTCPInit(t *testing.T) {
	assertProtocol(t, "tcp")
}

func TestUDPInit(t *testing.T) {
	assertProtocol(t, "udp")
}

func assertProtocol(t *testing.T, protocol string) {
	assert := assert.New(t)
	listener, err, r := createRoute(assert, protocol)
	defer listener.Close()
	_, err = NewLogstashAdapter(&r)
	assert.Nil(err)
}

func createRoute(assert *assert.Assertions, protocol string) (net.Listener, error, router.Route) {
	l, err := net.Listen("tcp", "localhost:0") // udp is not supported, use tcp for test purpose
	assert.Nil(err)
	var r router.Route
	r.Options = make(map[string]string)
	r.Address = l.Addr().String()
	r.Options["transport"] = protocol
	return l, err, r
}

func makeDummyContainer(id string) docker.Container {
	containerConfig := docker.Config{}
	containerConfig.Image = "image"
	containerConfig.Hostname = "hostname"

	container := docker.Container{}
	container.Name = "name"
	container.ID = id
	container.Config = &containerConfig

	return container
}

func pump(logstream chan *router.Message, container *docker.Container, structureLines [][]string) {
	for _, singleMessage := range structureLines {
		for _, line := range singleMessage {
			msg := makeDummyMessage(container, line)
			logstream <- &msg
		}
	}
	close(logstream)
}

func makeDummyMessage(container *docker.Container, data string) router.Message {
	return router.Message{
		Container: container,
		Source:    "FOOOOO",
		Data:      data,
		Time:      time.Now(),
	}
}

func parseResult(assert *assert.Assertions, serialized string) map[string]interface{} {
	var data map[string]interface{}
	err := json.Unmarshal([]byte(serialized), &data)
	assert.Nil(err)
	return data
}

func assertDockerInfo(assert *assert.Assertions, expected *docker.Container, actual interface{}) {
	var dockerInfo map[string]interface{}
	dockerInfo = actual.(map[string]interface{})
	assert.Equal(expected.Name, dockerInfo["name"])
	assert.Equal(expected.ID, dockerInfo["id"])
	assert.Equal(expected.Config.Image, dockerInfo["image"])
	assert.Equal(expected.Config.Hostname, dockerInfo["hostname"])
}
