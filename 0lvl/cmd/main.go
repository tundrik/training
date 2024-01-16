package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"time"

	"0lvl/config"
	"0lvl/internal/consumer"
	"0lvl/internal/endpoint"
	"0lvl/internal/repository"

	"github.com/ilyakaznacheev/cleanenv"
	"github.com/rs/zerolog"
)

func createLogger() zerolog.Logger {
	output := zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: time.RFC3339}
	return zerolog.New(output).With().Timestamp().Logger()
}

func main() {
	ctx, ctxCancel := context.WithCancel(context.Background())
	log := createLogger()

	var cfg config.Config
	if err := cleanenv.ReadEnv(&cfg); err != nil {
		log.Fatal().Err(err).Msg("")
	}

	repo, err := repository.New(ctx, log, cfg)
	if err != nil {
		log.Fatal().Err(err).Msg("")
	}
	defer repo.Close()
	
	go func() {
		err = endpoint.Run(ctx, repo, log)
		if err != nil {
			log.Fatal().Err(err).Msg("")
		}
	}()

	err = consumer.Run(repo, log, cfg)
	if err != nil {
		log.Fatal().Err(err).Msg("")
	}

	log.Info().Msg("[START SERVICE]")

	signals := make(chan os.Signal, 16)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM, syscall.SIGABRT)

	for {
		sign := <-signals
		log.Info().Str("signal", sign.String()).Msg("[STOP SERVICE]")
		ctxCancel()
		return
	}
}
