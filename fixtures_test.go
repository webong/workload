package workload

func validSpec() Spec {
	return Spec{
		AssignmentID:  "019d4000-0000-7000-8000-000000000001",
		ID:            "server-1",
		Type:          "test",
		Driver:        DriverLocal,
		DesiredState:  DesiredRunning,
		Revision:      1,
		Configuration: []byte(`{}`),
	}
}

type testEndpoints struct{}

func (testEndpoints) Add(Spec, string, []Endpoint) {}
func (testEndpoints) Remove(string, string)        {}
