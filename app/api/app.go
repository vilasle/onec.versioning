package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/ilyakaznacheev/cleanenv"
	"github.com/vilamslep/onec.versioning/lib"
	"github.com/vilamslep/onec.versioning/logger"
	"github.com/vilamslep/onec.versioning/raw"
)

type config struct {
	Socket struct {
		Host string `env:"APIHOST" env-default:"0.0.0.0"`
		Port int    `env:"APIPORT" env-default:"4000"`
	}
}

func main() {
	if err := lib.LoadEnv("dev.env"); err != nil {
		logger.Fatal("loading enviroment ", err)
	}

	logger.Info("config initialization")
	conf := config{}
	if err := cleanenv.ReadEnv(&conf); err != nil {
		head := "Onec Versioning: Service 'Api' "
		if desc, err := cleanenv.GetDescription(conf, &head); err == nil {
			logger.Fatal(desc)
		} else {
			logger.Fatal(err)
		}
	}

	if err := lib.CreateDatabaseConnection(); err != nil {
		logger.Fatal(err)
	}

	logger.Info("setting router")
	r := chi.NewRouter()
	r.Use(logger.MiddlewareLogger())
	r.Use(middleware.Recoverer)
	r.Use(middleware.RequestID)

	r.Route("/version", func(r chi.Router) {
		r.Post("/list", List)
		r.Get("/entity", EntityById)
	})

	logger.Info("start web service")
	http.ListenAndServe(fmt.Sprintf(":%d", conf.Socket.Port), r)
}

func List(w http.ResponseWriter, r *http.Request) {

	if r.Body == http.NoBody {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("request body is empty"))
		return
	}
	defer r.Body.Close()

	data, err := ioutil.ReadAll(r.Body)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("can not read body"))
		return
	}

	if len(data) == 0 {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("body is empty"))
	}

	if ok := raw.CheckRawDataRef(string(data)); !ok {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("body contents unexpected data"))
		return
	}

	ref := string(data)

	recs, err := lib.ListVersions(ref)

	if content, err := json.Marshal(recs); err == nil {
		w.Write(content)
	} else {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err.Error()))
	}
}

func EntityById(w http.ResponseWriter, r *http.Request) {
	id := r.URL.Query().Get("id")
	if id == "" {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("have not been define 'id' value"))
	}

	if v, err := lib.GetVersionById(id); err == nil {
		w.Write(v)
	} else {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("can not get version's record"))
	}
}
