package main

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"net/http"
	"time"

	"github.com/ilyakaznacheev/cleanenv"
	"github.com/vilamslep/iokafka"
	"github.com/vilamslep/onec.versioning/lib"
	"github.com/vilamslep/onec.versioning/logger"
)

const DEBUG = true

type Version struct {
	Ref       string
	Number    int
	User      string
	Content   []byte
	CreatedAt time.Time
}

type config struct {
	Kafka struct {
		Server string `env:"KAFKAHOST" env-default:"127.0.0.1"`
		Port   int    `env:"KAFKAPORT" env-default:"9092"`
		Topic  string `env:"KAFKATOPIC" env-required:"true"`
		Group  string `env:"KAFKAGROUP" env-required:"true"`
	}
	ProcessResourse struct {
		Server   string `env:"PROCHOST" env-required:"true"`
		Port     int    `env:"PROCPORT" env-required:"true"`
		Resourse string `env:"PROCURL" env-required:"true"`
		User     string `env:"PROCUSER" env-required:"true"`
		Password string `env:"PROCPASSWORD" env-required:"true"`
	}
}

func initKafkaConfig(conf config, offset int64) iokafka.ScannerConfig {

	host := conf.Kafka.Server
	port := conf.Kafka.Port
	topic := conf.Kafka.Topic
	group := conf.Kafka.Group

	soc := fmt.Sprintf("%s:%d", host, port)

	return iokafka.ScannerConfig{
		Brokers:       []string{soc},
		Topic:         topic,
		GroupID:       group,
		AttemtsOnFail: 5,
		FailTimeout:   10,
		StartOffset:   offset,
	}
}

func main() {

	if DEBUG {
		lib.LoadEnv("dev.env")
	}

	logger.Info("config initialization")
	conf := config{}
	if err := cleanenv.ReadEnv(&conf); err != nil {
		head := "Onec Versioning: Service 'Process' "
		if desc, err := cleanenv.GetDescription(conf, &head); err == nil {
			logger.Fatal(desc)
		} else {
			logger.Fatal(err)
		}
	}

	scanner := iokafka.NewScanner(initKafkaConfig(conf, 0))
	if err := lib.CreateDatabaseConnection(); err != nil {
		logger.Fatal(err)
	}

	resrs := fmt.Sprintf("http://%s:%d/%s",
		conf.ProcessResourse.Server, conf.ProcessResourse.Port, conf.ProcessResourse.Resourse)

	for scanner.Scan() {
		msg := scanner.Message()

		req, err := http.NewRequest(http.MethodPost, resrs, bytes.NewReader(msg.Value))
		if err != nil {
			logger.Error(err)
		}
		req.SetBasicAuth(conf.ProcessResourse.User, conf.ProcessResourse.Password)

		if content, err := requestNewVersion(req); err == nil {
			version := Version{
				Ref:       string(msg.Value),
				User:      string(msg.Key),
				Content:   content,
				CreatedAt: time.Now(),
			}
			if err := version.SaveVersion(); err == nil {
				logger.Infof("add new version. Ref %s. Number %d", version.Ref, version.Number)
			} else {
				logger.Error(err)
			}
		} else {
			logger.Error(err)
		}
	}
	logger.Info("finish application")
}

func requestNewVersion(request *http.Request) ([]byte, error) {

	c := http.Client{
		Timeout: time.Second * 300,
	}

	if res, err := c.Do(request); err != nil {
		return nil, err
	} else {
		if res.Body == http.NoBody {
			return nil, fmt.Errorf("response does not have body")
		}
		defer res.Body.Close()

		content, err := ioutil.ReadAll(res.Body)
		if err != nil {
			return nil, err
		}

		if res.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("not success request. Code %d, Body %s", res.StatusCode, content)
		}

		return content, nil
	}
}

func (v *Version) SaveVersion() error {

	if lastNumber, err := lib.GetLastIdByRef(v.Ref); err == nil {
		v.Number = lastNumber + 1
	} else {
		return err
	}

	q := `INSERT INTO public.versions(ref, version_number, version_user, content, created_at)
		VALUES($1, $2, $3, $4, $5);`

	return lib.ExecQuery(q, v.Ref, v.Number, v.User, v.Content, v.CreatedAt)
}
