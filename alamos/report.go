package alamos

import (
	"encoding/json"
	"io"
)

type Report map[string]interface{}

// JSON writes the report as JSON as bytes.
func (r Report) JSON() ([]byte, error) {
	return json.Marshal(r)
}

// WriteJSON writes the report as JSON to the given writer.
func (r Report) WriteJSON(w io.Writer) error {
	return json.NewEncoder(w).Encode(r)
}

// Report implements Experiment.
func (e *experiment) Report() Report {
	report := make(map[string]interface{})
	for k, v := range e.measurements {
		report[k] = v.Report()
	}
	for k, v := range e.children {
		report[k] = v.Report()
	}
	return report
}
