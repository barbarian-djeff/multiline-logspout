package multiline

import (
	"fmt"
	"github.com/gliderlabs/logspout/router"
	"regexp"
	"strings"
	"time"
)

type MultilineConfig struct {
	Pattern   *regexp.Regexp `config:"pattern"     validate:"required"`
	GroupWith string         `config:"match"       validate:"required"`
	Negate    bool           `config:"negate"`
	Separator *string        `config:"separator"`
	MaxLines  int            `config:"max_lines"`
}

// MultiLine processor combining multiple line events into one multi-line event.
//
// Lines to be combined are matched by some configurable predicate using
// regular expression.
//
// The maximum number of lines to be returned is fully configurable.
// Even if limits are reached subsequent lines are matched, until event is
// fully finished.
type MultiLine struct {
	isMultiline matcher
	maxLines    int
	separator   string

	pending     []*router.Message
	LastTouched time.Time
}

const (
	// Default maximum number of lines to return in one multi-line event
	defaultMaxLines = 1 << 10

	defaultSeparator = "\n"
)

// Matcher represents the predicate comparing any two lines
// to find start and end of multiline events in stream of line events.
type matcher func(lastText, currentText string) bool

// NewMultiLine creates a new multi-line processor combining stream of
// line events into stream of multi-line events.
func NewMultiLine(config *MultilineConfig) (MultiLine, error) {
	types := map[string]func(*regexp.Regexp) (matcher, error){
		"next":     nextMatcher,
		"previous": previousMatcher,
	}

	matcherType, ok := types[config.GroupWith]
	if !ok {
		return MultiLine{}, fmt.Errorf("unknown matcher type: %s", config.GroupWith)
	}

	matcher, err := matcherType(config.Pattern)
	if err != nil {
		return MultiLine{}, err
	}

	if config.Negate {
		matcher = negatedMatcher(matcher)
	}

	maxLines := defaultMaxLines
	if config.MaxLines > 0 {
		maxLines = config.MaxLines
	}

	separator := defaultSeparator
	if config.Separator != nil {
		separator = *config.Separator
	}

	ml := MultiLine{
		isMultiline: matcher,
		separator:   separator,
		maxLines:    maxLines,
	}
	return ml, nil
}

// Adds a message to the MultiLine buffer, returning a flushed message if one is ready
func (ml *MultiLine) Buffer(next *router.Message) *router.Message {
	ml.LastTouched = time.Now()
	if ml.isContinuationMessage(next) {
		return ml.addPending(next)
	} else {
		return ml.StartNewLine(next)
	}
}

func (ml *MultiLine) isContinuationMessage(msg *router.Message) bool {
	return ml.PendingSize() == 0 ||
		ml.isMultiline(ml.getLastPendingData(), msg.Data)
}

func (ml *MultiLine) addPending(next *router.Message) *router.Message {
	if ml.PendingSize() < ml.maxLines {
		ml.pending = append(ml.pending, next)
	} else if ml.PendingSize() == ml.maxLines {
		truncMessage := *next
		truncMessage.Data = "[Truncated]"
		ml.pending = append(ml.pending, &truncMessage)
	}

	return nil
}

func (ml *MultiLine) StartNewLine(next *router.Message) *router.Message {
	msg := ml.Flush()
	ml.pending = []*router.Message{next}

	return msg
}

func (ml *MultiLine) Flush() *router.Message {
	var buffer []string
	var msg *router.Message

	for _, message := range ml.pending {
		buffer = append(buffer, message.Data)
	}

	if ml.PendingSize() > 0 {
		msg = new(router.Message)
		*msg = *ml.pending[0]
		msg.Data = strings.Join(buffer, ml.separator)
	}

	return msg
}

func (ml *MultiLine) PendingSize() int {
	return len(ml.pending)
}

func (ml *MultiLine) Expire(t time.Time, ttl time.Duration) *router.Message {
	if isExpired(t, ml.LastTouched, ttl) {
		return ml.Flush()
	} else {
		return nil
	}
}

func isExpired(t time.Time, lastTouched time.Time, ttl time.Duration) bool {
	return t.Sub(lastTouched) > ttl
}

func (ml *MultiLine) getLastPendingData() string {
	return ml.pending[ml.PendingSize()-1].Data
}

// matchers

func previousMatcher(regex *regexp.Regexp) (matcher, error) {
	return genPatternMatcher(regex, func(lastText, currentText string) string {
		return currentText
	})
}

func nextMatcher(regex *regexp.Regexp) (matcher, error) {
	return genPatternMatcher(regex, func(lastText, currentText string) string {
		return lastText
	})
}

func negatedMatcher(m matcher) matcher {
	return func(lastText, currentText string) bool {
		return !m(lastText, currentText)
	}
}

func genPatternMatcher(
	regex *regexp.Regexp,
	sel func(lastText, currentText string) string,
) (matcher, error) {
	matcher := func(lastText, currentText string) bool {
		line := sel(lastText, currentText)
		return regex.MatchString(line)
	}
	return matcher, nil
}
