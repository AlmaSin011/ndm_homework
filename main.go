package main

import (
	"flag"
	"log"
	"net/http"
	"strconv"
	"sync"
	"time"
)

type Queue struct {
	mu       sync.Mutex
	messages []string
	waiters  []chan string
}

var (
	queues   = map[string]*Queue{}
	queuesMu sync.Mutex
)

func getQueue(name string) *Queue {
	queuesMu.Lock()
	defer queuesMu.Unlock()
	q, ok := queues[name]
	if !ok {
		q = &Queue{}
		queues[name] = q
	}
	return q
}

func main() {
	port := flag.String("port", "8080", "port to listen")
	flag.Parse()

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		name := r.URL.Path[1:]
		q := getQueue(name)

		switch r.Method {
		case http.MethodPut:
			v := r.URL.Query().Get("v")
			if v == "" {
				http.Error(w, "", http.StatusBadRequest)
				return
			}
			q.mu.Lock()
			if len(q.waiters) > 0 {
				ch := q.waiters[0]
				q.waiters = q.waiters[1:]
				q.mu.Unlock()
				ch <- v
			} else {
				q.messages = append(q.messages, v)
				q.mu.Unlock()
			}

		case http.MethodGet:
			t, err := strconv.Atoi(r.URL.Query().Get("timeout"))
			q.mu.Lock()
			if len(q.messages) > 0 {
				msg := q.messages[0]
				q.messages = q.messages[1:]
				q.mu.Unlock()
				w.Write([]byte(msg))
				return
			}
			if err != nil {
				q.mu.Unlock()
				http.Error(w, "", http.StatusNotFound)
				return
			}
			ch := make(chan string, 1)
			q.waiters = append(q.waiters, ch)
			q.mu.Unlock()

			select {
			case msg := <-ch:
				w.Write([]byte(msg))
			case <-time.After(time.Duration(t) * time.Second):
				q.mu.Lock()
				for i, w := range q.waiters {
					if w == ch {
						q.waiters = append(q.waiters[:i], q.waiters[i+1:]...)
						break
					}
				}
				q.mu.Unlock()
				http.Error(w, "", http.StatusNotFound)
			}

		default:
			http.Error(w, "", http.StatusMethodNotAllowed)
		}
	})

	if err := http.ListenAndServe(":"+*port, nil); err != nil {
		log.Fatal(err)
	}
}
