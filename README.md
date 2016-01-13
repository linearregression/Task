# AWS SQS Task Scheduler

## Config

environment configuration (Or .env file)

```
AWS_DEFAULT_REGION=
AWS_ACCESS_KEY_ID=
AWS_SECRET_ACCESS_KEY=
AWS_SQS_QUEUE=
```

## Example

```
package main

import (
    "github.com/gotoolz/task"
    "time"
)

// Create your task
var helloWorldTask = task.New("hello-world", func(firstParam, secondParam string) {

    fmt.Printf("\n\n[Hello World, %s %s]\n\n", firstParam, secondParam)
})

func main() {

    // Scheduled right now
    helloWorldTask.Schedule("Foo", "Bar")

    // Scheduled in 60 seconds
    helloWorldTask.ScheduleWithDelay(60, "60", "Seconds")

    // Wait for task
    time.Sleep(90 * time.Second)
}
```
