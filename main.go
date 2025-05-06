package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/rabbitmq/amqp091-go"
)

type Item struct {
	IndicatorId     string `json:"indicator_id"`
	IndicatorValue  string `json:"indicator_value"`
	CountryId       string `json:"country_id"`
	CountryValue    string `json:"country_value"`
	CountryISO3Code string `json:"country_iso3_code"`
	Date            string `json:"date"`
	Value           uint   `json:"value"`
	Unit            string `json:"unit"`
	ObsStatus       string `json:"obs_status"`
	Decimal         uint   `json:"decimal"`
}

var history []Item

func main() {
	conn, err := amqp091.Dial("amqp://guest:guest@localhost:5672/")
	if err != nil {
		log.Fatalf("Не удалось подключиться к RabbitMQ: %v", err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("Не удалось открыть канал: %v", err)
	}
	defer ch.Close()

	err = ch.ExchangeDeclare(
		"broadcast", // имя exchange
		"fanout",    // тип exchange
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		log.Fatalf("Не удалось объявить exchange: %v", err)
	}

	q, err := ch.QueueDeclare(
		"",
		false,
		true,
		true,
		false,
		nil,
	)
	if err != nil {
		log.Fatalf("❌ Не удалось объявить очередь: %v", err)
	}

	err = ch.QueueBind(
		q.Name,
		"",
		"broadcast",
		false,
		nil,
	)
	if err != nil {
		log.Fatalf("❌ Не удалось привязать очередь к exchange: %v", err)
	}

	msgs, err := ch.Consume(
		q.Name,
		"",
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		log.Fatalf("❌ Не удалось начать потребление: %v", err)
	}

	log.Printf("🟢 Подписчик запущен. Ожидаем новые элементы...\n")
	log.Println("ℹ️ Введите `last` для запроса последней записи или `exit` для выхода.")

	// forever := make(chan bool)

	go func() {
		for d := range msgs {
			var item Item
			err := json.Unmarshal(d.Body, &item)
			if err != nil {
				log.Printf("⚠️ Ошибка при разборе JSON: %v", err)
				continue
			}
			history = append(history, item)
			fmt.Printf("📬 Получено новое сообщение:\n%+v\n\n", item)
		}
	}()

	// <-forever

	reader := bufio.NewReader(os.Stdin)
	for {
		fmt.Print(">>> ")
		text, _ := reader.ReadString('\n')
		text = strings.TrimSpace(text)

		switch text {
		case "last":
			if len(history) == 0 {
				fmt.Println("📭 Сообщений пока нет.")
			} else {
				last := history[len(history)-1]
				fmt.Printf("📦 Последнее сообщение:\n%+v\n\n", last)
			}
		case "exit":
			fmt.Println("👋 Выход.")
			return
		default:
			fmt.Println("❓ Неизвестная команда. Доступны: last, exit")
		}
	}
}
