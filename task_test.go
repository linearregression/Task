package task

import (
	"fmt"
	"testing"
	"time"
)

// Global changed if task is executed
var ok bool = false

// Sample task
var sample = New("sample-task", func(firstname, lastname string) {

	fmt.Printf("\n\n[Hello %s %s]\n\n", firstname, lastname)
	ok = true
})

// Test task schedule and execute
func TestMessage(t *testing.T) {

	fmt.Printf("schedule task\n")
	sample.Schedule("Foo", "Bar")

	time.Sleep(5 * time.Second)

	if !ok {
		t.Fatalf("didn't received task...\n")
	}
}
