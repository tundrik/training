package consumer

import (
	"0lvl/config"
	"0lvl/internal/repository"

	"github.com/rs/zerolog"
	stan "github.com/nats-io/stan.go"
)


func Run(repo *repository.Repo, log zerolog.Logger, cfg config.Config) error {
	sc, err := stan.Connect(cfg.StanClusterId, cfg.StanClientId); if err != nil {
        return err
	}

	handler := func(m *stan.Msg) {
		err := repo.SaveOrder(m.Data); if err != nil {
			log.Err(err).Msg("")
		}
	    //Даже если Repo вернул ошибку, помечается как обработанный
        err = m.Ack(); if err != nil {
			log.Err(err).Msg("")
		}
	}

	_, err = sc.Subscribe(cfg.StanSubject, handler, stan.SetManualAckMode(), stan.DurableName(cfg.StanClientId)); if err != nil {
		return err
	}
	return nil
}
