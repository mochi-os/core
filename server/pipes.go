// Mochi server: Pipes with timeouts
// Copyright Alistair Cunningham 2025

package main

import (
	"io"
	"os"
	"sync"
	"time"
)

type pipe_reader struct {
	*io.PipeReader
	mu       sync.Mutex
	deadline time.Time
}

type pipe_writer struct {
	*io.PipeWriter
	mu       sync.Mutex
	deadline time.Time
}

func (r *pipe_reader) SetReadDeadline(t time.Time) error {
	r.mu.Lock()
	r.deadline = t
	r.mu.Unlock()
	return nil
}

func (w *pipe_writer) SetWriteDeadline(t time.Time) error {
	w.mu.Lock()
	w.deadline = t
	w.mu.Unlock()
	return nil
}

func (r *pipe_reader) Read(p []byte) (int, error) {
	r.mu.Lock()
	d := r.deadline
	r.mu.Unlock()

	if d.IsZero() {
		return r.PipeReader.Read(p)
	}

	timeout := time.Until(d)
	if timeout <= 0 {
		return 0, os.ErrDeadlineExceeded
	}

	done := make(chan struct {
		n   int
		err error
	}, 1)

	go func() {
		n, err := r.PipeReader.Read(p)
		done <- struct {
			n   int
			err error
		}{n, err}
	}()

	select {
	case r := <-done:
		return r.n, r.err

	case <-time.After(timeout):
		return 0, os.ErrDeadlineExceeded
	}
}

func (w *pipe_writer) Write(p []byte) (int, error) {
	w.mu.Lock()
	d := w.deadline
	w.mu.Unlock()

	if d.IsZero() {
		return w.PipeWriter.Write(p)
	}

	timeout := time.Until(d)
	if timeout <= 0 {
		return 0, os.ErrDeadlineExceeded
	}

	done := make(chan struct {
		n   int
		err error
	}, 1)

	go func() {
		n, err := w.PipeWriter.Write(p)
		done <- struct {
			n   int
			err error
		}{n, err}
	}()

	select {
	case r := <-done:
		return r.n, r.err

	case <-time.After(timeout):
		return 0, os.ErrDeadlineExceeded
	}
}
