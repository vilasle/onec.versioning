package main

import (
	"io/ioutil"
	"net/http"
	"regexp"
	"strings"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/vilamslep/iokafka"
)

type Handler func(w http.ResponseWriter, r *http.Request) error

func (h Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if err := h(w, r); err != nil {
		w.WriteHeader(503)
		w.Write([]byte(err.Error()))
	}
}

func main() {

	wrt := iokafka.NewWriter("172.19.1.3:9092", "raw")
	r := chi.NewRouter()
	r.Use(middleware.Logger)
	r.Use(middleware.Recoverer)
	r.Use(middleware.RequestID)

	r.Method("POST", "/raw", HandlerRawProducer(wrt))

	http.ListenAndServe(":3000", r)
}

func HandlerRawProducer(wrt *iokafka.Writer) Handler {
	return func(w http.ResponseWriter, r *http.Request) error {

		b, err := ioutil.ReadAll(r.Body)
		if err != nil {
			return err
		}

		content := strings.ReplaceAll(string(b), "\n", "")

		regStr := `^[[:xdigit:]]{8}(-[[:xdigit:]]{4}){3}-[[:xdigit:]]{12};{"#",+[[:xdigit:]]{8}(-[[:xdigit:]]{4}){3}-[[:xdigit:]]{12},[\d]{1,6}:[[:xdigit:]]{32}}$`

		matched, err := regexp.MatchString(regStr, content)

		if err != nil {
			return err
		}

		if !matched {
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte("the body does not match the expected format"))
			return nil
		}

		pathContent := strings.Split(content, ";")
		user, data := pathContent[0], pathContent[1]

		msg := iokafka.Message{
			Key:   []byte(user),
			Value: []byte(data),
		}

		err = wrt.Write(msg)

		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(err.Error()))
		} else {
			w.Write([]byte("success"))
		}
		return nil
	}
}