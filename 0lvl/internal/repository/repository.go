package repository

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"unsafe"

	"0lvl/config"
	"0lvl/pkg/cache"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog"
)


const (
	// maxCacheBytes максимальный размер кеша (FIFO).
	maxCacheBytes = 1024 * 1024 * 32

	// entryByteSize примерный размер кешированного ордера в байтах.
	// если это будет меньше чем по факту то запрос в db при старте будет с большим лимитом
	// чем влезет в кеш
	entryByteSize = 1200

	// initCacheCount количество ордеров из db для прогрева кеша при старте.
	initCacheCount = maxCacheBytes / entryByteSize
)


type Repo struct {
	db    *pgxpool.Pool
	log   zerolog.Logger
	cache *cache.Cache
}

func New(ctx context.Context, log zerolog.Logger, cfg config.Config) (*Repo, error) {
	db, err := pgxpool.New(ctx, cfg.PgString); if err != nil {
		return nil, err
	}
	cache, err := cache.New(maxCacheBytes); if err != nil {
		return nil, err
	}
    repo := &Repo{
		db: db,
		log: log,
		cache: cache,
	}

	go repo.cacheWarmUp()

	return repo, nil
}

func (r *Repo) Close() {
	r.db.Close()
}

func (r *Repo) SaveOrder(msg []byte) error {
    var d Order
	err := json.Unmarshal(msg, &d); if err != nil {
		return err
	}
	const sql = `INSERT INTO trade (pk, rang, entity) VALUES ($1, $2, $3);`
	_, err = r.db.Exec(context.Background(), sql, d.OrderUid, d.DateCreated.UnixMicro(), msg); if err != nil {
		return err
	}

    err = r.cache.Set(s2b(d.OrderUid), msg); if err != nil {
        r.log.Err(err).Msg("")
	}

	return nil
}

func (r *Repo) GetOrderByUid(uid string) ([]byte, error) {
    b, ok := r.cache.HasGet(nil, s2b(uid)); if ok {
        return b, nil
	}

	var order any
	const sql = `SELECT entity FROM trade WHERE pk = $1;`
    err := r.db.QueryRow(context.Background(), sql, uid).Scan(&order); if err != nil {
		return nil, err
	}
	
	b, _ = json.Marshal(order)
    return b, nil
}

func (r *Repo) GetOrderList(count int) []byte {
	const sql = `SELECT pk, rang FROM trade ORDER BY rang DESC LIMIT $1;`
    rows, err := r.db.Query(context.Background(), sql, count); if err != nil {
		r.log.Err(err).Msg("")
	}
	defer rows.Close()

	entities := make([]OrderLink, 0, count)

	for rows.Next() {
		rowValues := rows.RawValues()
		pk := string(rowValues[0])
		entity := OrderLink{
			Uid: pk,
			Link: "http://localhost:8000/order/" + pk,
			Rank: binary.BigEndian.Uint64(rowValues[1]),
		}
		entities = append(entities, entity)
	}

	b, _ := json.Marshal(entities)
    return b
}

func (r *Repo) Metric() []byte {
	var m Monitor 
	r.cache.UpdateStats(&m.Cache)


	const sql = `SELECT count(pk) FROM trade;`
    err := r.db.QueryRow(context.Background(), sql).Scan(&m.DatabaseOrderCount); if err != nil {
		r.log.Err(err).Msg("")
	}

    mb, _ := json.Marshal(m)
    return mb
}

func (r *Repo) cacheWarmUp() {
	const sql = `SELECT pk, rang, entity FROM trade ORDER BY rang DESC LIMIT $1;`
    rows, err := r.db.Query(context.Background(), sql, initCacheCount); if err != nil {
		r.log.Err(err).Msg("db error")
	}
	defer rows.Close()

	for rows.Next() {
		rowValues := rows.RawValues()
		err := r.cache.Set(rowValues[0], rowValues[2]); if err != nil {
			r.log.Err(err).Msg("")
		}
	}
	r.log.Info().Msg("done cache warm up")
}


func s2b(s string) []byte {
	return unsafe.Slice(unsafe.StringData(s), len(s))
}