package scheduler

// EventPredicate funcs evaluate scheduler events and determine some pass/fail outcome; useful for
// filtering or conditionally handling events.
type EventPredicate func(*Event) bool

// Apply returns the result of the predicate function; always true if the predicate is nil.
func (ep EventPredicate) Apply(e *Event) (result bool) {
	if ep == nil {
		result = true
	} else {
		result = ep(e)
	}
	return
}

// Predicate implements scheduler/events.Predicate
func (t Event_Type) Predicate() EventPredicate { return func(e *Event) bool { return e.GetType() == t } }
