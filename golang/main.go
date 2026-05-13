package main

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

type Result[T any] struct {
	Value T
	Err   error
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	out := Map(ctx, Stream(ctx, 1, 20), 4, func(ctx context.Context, n int) (int, error) {
		select {
		case <-time.After(time.Duration(n%5+1) * 30 * time.Millisecond):
			return n * n, nil
		case <-ctx.Done():
			return 0, ctx.Err()
		}
	})

	sum, err := Reduce(out, 0, func(acc int, r Result[int]) (int, error) {
		if r.Err != nil {
			return acc, r.Err
		}
		return acc + r.Value, nil
	})
	if err != nil && !errors.Is(err, context.Canceled) {
		panic(err)
	}

	fmt.Println(sum)
}

func Stream(ctx context.Context, from, to int) <-chan int {
	out := make(chan int)
	go func() {
		defer close(out)
		for i := from; i <= to; i++ {
			select {
			case out <- i:
			case <-ctx.Done():
				return
			}
		}
	}()
	return out
}

func Map[T, R any](
	ctx context.Context,
	in <-chan T,
	parallelism int,
	fn func(context.Context, T) (R, error),
) <-chan Result[R] {
	out := make(chan Result[R])
	var wg sync.WaitGroup

	worker := func() {
		defer wg.Done()
		for item := range in {
			value, err := fn(ctx, item)
			select {
			case out <- Result[R]{Value: value, Err: err}:
			case <-ctx.Done():
				return
			}
		}
	}

	wg.Add(parallelism)
	for range parallelism {
		go worker()
	}

	go func() {
		wg.Wait()
		close(out)
	}()

	return out
}

func Reduce[T, A any](in <-chan T, seed A, fn func(A, T) (A, error)) (A, error) {
	acc := seed
	for item := range in {
		next, err := fn(acc, item)
		if err != nil {
			return acc, err
		}
		acc = next
	}
	return acc, nil
}
