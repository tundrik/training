package endpoint

import (
	"context"
	"net/http"

	"0lvl/internal/repository"

	"github.com/julienschmidt/httprouter"
	"github.com/rs/zerolog"
)


var (
	msgNoData = []byte(`{"message": "No data"}`)
)


type Endpoint struct {
    repo *repository.Repo
	log   zerolog.Logger
}

func Run(ctx context.Context, repo *repository.Repo, log zerolog.Logger) error {
	h := Endpoint{
		repo: repo,
		log: log,
	}
    router := httprouter.New()
    router.GET("/", h.index)
    router.GET("/order/:uid", h.order)
	router.GET("/metric", h.metric)

	server := &http.Server{
		Addr:    ":8000",
		Handler: router,
	}

	return server.ListenAndServe()
}



func (h *Endpoint) index(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
    b := h.repo.GetOrderList(32)
	w.Write(b)
}

func (h *Endpoint) order(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	b, err := h.repo.GetOrderByUid(ps.ByName("uid")); if err != nil {
		w.WriteHeader(404)
        w.Write(msgNoData)
        return
	}
	w.Write(b)
}

func (h *Endpoint) metric(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	b := h.repo.Metric()
	w.Write(b)
}