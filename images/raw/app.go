package main

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"regexp"
	"strings"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/vilamslep/iokafka"
	"github.com/vilamslep/onec.versioning/lib"
	"github.com/vilamslep/onec.versioning/logger"
)

type Handler func(w http.ResponseWriter, r *http.Request) error

func (h Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if err := h(w, r); err != nil {
		logger.Errorf("Error on handling ", err)
		w.WriteHeader(503)
		w.Write([]byte(err.Error()))
	}
}

func getKafkaWrite() *iokafka.Writer {
	host := os.Getenv("KAFKAHOST")
	port := os.Getenv("KAFKAPORT")
	topic := os.Getenv("KAFKATOPIC")

	return iokafka.NewWriter(fmt.Sprintf("%s:%s", host, port), topic)
}

func main() {
	lib.LoadEnv("dev.env")

	logger.Info("Start web service")

	wrt := getKafkaWrite()
	r := chi.NewRouter()
	r.Use(logger.MiddlewareLogger())
	r.Use(middleware.Recoverer)
	r.Use(middleware.RequestID)

	resourse := os.Getenv("RAWRESOURSE")
	port := os.Getenv("RAWPORT")
	r.Method("POST", "/"+resourse, HandlerRawProducer(wrt))

	http.ListenAndServe(":"+port, r)
}

func HandlerRawProducer(wrt *iokafka.Writer) Handler {
	return func(w http.ResponseWriter, r *http.Request) error {

		if r.Body == http.NoBody {
			return fmt.Errorf("request does not have body")
		}

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
			w.Write([]byte("ok"))
		}
		return nil
	}
}

