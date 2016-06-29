package main

import (
    "flag"
    "fmt"
    "os"
    "time"
    "git.rescrv.net/replicant/bindings/go"
    "git.rescrv.net/ygor/bindings/go"
)

func consume(errChan chan replicant.Error) {
    for e := range errChan {
        fmt.Printf("error: %s\n", e)
        os.Exit(1)
    }
}

func work(c *replicant.Client, C <-chan time.Time, stop chan bool, done chan bool, dl *ygor.DataLogger) {
    defer func() {
        done<-true
    }()
    for {
        select {
        case <- C:
            break
        case <- stop:
            return
        }
        start := time.Now()
        _, err := c.Call("echo", "echo", []byte("hello world"), 0)
        end := time.Now()
        if err.Status == replicant.SUCCESS {
            when := uint64(end.UnixNano())
            data := uint64(end.Sub(start).Nanoseconds())
            er := dl.Record(1, when, data)
            if er != nil {
                fmt.Printf("error: %s\n", er)
                os.Exit(1)
            }
        } else {
            fmt.Printf("error: %s\n", err)
            os.Exit(1)
        }
    }
}

func main() {
    connStr := flag.String("connect", "127.0.0.1:1982", "cluster connection string")
    targetTput := flag.Float64("target", 1000, "target throughput")
    maxConcurrent := flag.Int("max-concurrent", 1000, "maximum concurrent operations")
    output := flag.String("output", "benchmark.dat.bz2", "benchmark output")
    flag.Parse()
    ticker := time.NewTicker(time.Second / time.Duration(*targetTput * 1.01))
    stopper := make(chan bool, 1)
    done := make(chan bool, *maxConcurrent)
    c, er, errChan := replicant.NewClient(*connStr)
    if er != nil {
        fmt.Printf("error: %s\n", er)
        os.Exit(1)
    }
    dl, err := ygor.NewDataLogger(*output, uint64(time.Millisecond), uint64(time.Microsecond * 100))
    if err != nil {
        fmt.Printf("error: %s\n", err)
        os.Exit(1)
    }
    go consume(errChan)
    for i := 0; i < *maxConcurrent; i++ {
        go work(c, ticker.C, stopper, done, dl)
    }
    time.Sleep(time.Duration(60) * time.Second)
    ticker.Stop()
    for i := 0; i < *maxConcurrent; i++ {
        stopper<-true
        <-done
    }
    err = dl.FlushAndDestroy()
    if err != nil {
        fmt.Printf("error: %s\n", err)
        os.Exit(1)
    }
    os.Exit(0)
}
