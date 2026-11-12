package main

import (
	"flag"
	"fmt"
	"github.com/webong/workload"
	"os"
	"time"
)

func main() {
	dir := flag.String("state-dir", "", "private durable state directory")
	key := flag.String("key", "", "assignment identity hash")
	timeout := flag.Duration("stop-timeout", 5*time.Second, "total process shutdown budget")
	flag.Parse()
	if err := workload.ServeLocalSupervisor(*dir, *key, *timeout); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
