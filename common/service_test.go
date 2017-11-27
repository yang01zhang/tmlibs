package common

import (
	"testing"
)

func TestBaseServiceWait(t *testing.T) {

	type TestService struct {
		BaseService
	}
	ts := &TestService{}
	ts.BaseService = *NewBaseService("TestService", ts, nil)
	ts.Start()

	go func() {
		ts.Stop()
	}()

	for i := 0; i < 10; i++ {
		ts.Wait()
	}

}
