package main

import (
	"context"
	"fmt"

	trace "github.com/lechandonga/trace-collection-pipeline-go/trace"
)

func main() {
	_ = context.Background()
	_ = trace.TraceID{}
	fmt.Println("trace collection pipeline demo (skeleton)")
}
