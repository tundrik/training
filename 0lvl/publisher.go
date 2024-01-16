package main

import (
	"crypto/rand"
	"encoding/json"
	"os"
	"strings"
	"time"
	"unsafe"

	fake "github.com/brianvoe/gofakeit/v6"
	stan "github.com/nats-io/stan.go"
	"github.com/rs/zerolog"
)

func createLogger() zerolog.Logger {
	output := zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: time.RFC3339}
	return zerolog.New(output).With().Timestamp().Logger()
}

type Delivery struct {
	Name    string `json:"name"`
	Phone   string `json:"phone"`
	Zip     string `json:"zip"`
	City    string `json:"city"`
	Address string `json:"address"`
	Region  string `json:"region"`
	Email   string `json:"email"`
}

type Payment struct {
	Transaction  string `json:"transaction"`
	RequestId    string `json:"request_id"`
	Currency     string `json:"currency"`
	Provider     string `json:"provider"`
	Amount       int    `json:"amount"`
	PaymentDt    int    `json:"payment_dt"`
	Bank         string `json:"bank"`
	DeliveryCost int    `json:"delivery_cost"`
	GoodsTotal   int    `json:"goods_total"`
	CustomFee    int    `json:"custom_fee"`
}

type Item struct {
	ChrtId      int    `json:"chrt_id"`
	TrackNumber string `json:"track_number"`
	Price       int    `json:"price"`
	Rid         string `json:"rid"`
	Name        string `json:"name"`
	Sale        int    `json:"sale"`
	Size        string `json:"size"`
	TotalPrice  int    `json:"total_price"`
	NmId        int    `json:"nm_id"`
	Brand       string `json:"brand"`
	Status      int    `json:"status"`
}

type Order struct {
	OrderUid          string `json:"order_uid"`
	TrackNumber       string `json:"track_number"`
	Entry             string `json:"entry"`
	Delivery          Delivery  `json:"delivery"`
	Payment           Payment   `json:"payment"`
	Items             []Item    `json:"items"`
	Locale            string    `json:"locale"`
	InternalSignature string    `json:"internal_signature"`
	CustomerId        string    `json:"customer_id"`
	DeliveryService   string    `json:"delivery_service"`
	Shardkey          string    `json:"shardkey"`
	SmId              int       `json:"sm_id"`
	DateCreated       time.Time `json:"date_created"`
	OofShard          string    `json:"oof_shard"`
}

func genOrder() Order {
	items := make([]Item, 0)

	entry := "WBIL"
	track := entry + strings.ToUpper(nonceGenerate(16))

	orderId := nonceGenerate(16)
	customerId := nonceGenerate(16)
	orderUid := orderId + customerId

	for i := 0; i < 2; i++ {
		item := Item{
			ChrtId:      fake.Number(1, 9999999),
			TrackNumber: track,
			Price:       199,
			Rid:         nonceGenerate(16) + customerId,
			Name:        fake.ProductName(),
			Sale:        0,
			Size:        "0",
			TotalPrice:  199,
			NmId:        fake.Number(1, 9999999),
			Brand:       fake.Company(),
			Status:      0,
		}
		items = append(items, item)
	}

	order := Order{
		OrderUid:    orderUid,
		TrackNumber: track,
		Entry:       entry,
		Delivery: Delivery{
			Name:    fake.Name(),
			Phone:   "+" + fake.PhoneFormatted(),
			Zip:     fake.Zip(),
			City:    fake.City(),
			Address: fake.Street(),
			Region:  fake.State(),
			Email:   fake.Email(),
		},
		Payment: Payment{
			Transaction:  orderUid,
			RequestId:    "",
			Currency:     fake.CurrencyShort(),
			Provider:     "wbpay",
			Amount:       3000,
			PaymentDt:    0,
			Bank:         "SberBank",
			DeliveryCost: 2403,
			GoodsTotal:   597,
			CustomFee:    0,
		},
		Items:             items,
		Locale:            fake.LanguageAbbreviation(),
		InternalSignature: "",
		CustomerId:        customerId,
		DeliveryService:   fake.RandomString([]string{"meest", "nova poshta"}),
		Shardkey:          "9",
		SmId:              99,
		DateCreated:       time.Now(),
		OofShard:          "1",
	}

	
	return order
}

func main() {
	time.Local = time.UTC

	logger := createLogger()

	sc, err := stan.Connect("test-cluster", "client-2"); if err != nil {
		logger.Fatal().Err(err).Msg("")
	}
	defer sc.Close()

	for {
		dataOrder := genOrder()

		b, _ := json.Marshal(dataOrder)

		err = sc.Publish("order", b); if err != nil {
			logger.Fatal().Err(err).Msg("")
		}

        logger.Info().Str("uid", dataOrder.OrderUid).Msg("publish order")
		time.Sleep(100 * time.Millisecond) 
	}
}

var alphaWb = []byte("abcdefghijklmnopqrstuvwxyz123456789")

func nonceGenerate(size int) string {
	b := make([]byte, size)
	rand.Read(b)
	for i := 0; i < size; i++ {
		b[i] = alphaWb[b[i]%byte(len(alphaWb))]
	}
	return *(*string)(unsafe.Pointer(&b))
}
