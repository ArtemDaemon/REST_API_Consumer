package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"

	"github.com/joho/godotenv"
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

const apiURL = "http://localhost:8080/api/last_item"

func getLastItem(token string) {
	req, err := http.NewRequest("GET", apiURL, nil)
	if err != nil {
		log.Printf("❌ Ошибка создания запроса: %v", err)
		return
	}

	req.Header.Set("Authorization", "Bearer "+token)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Printf("❌ Ошибка при выполнении запроса: %v", err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		log.Printf("⚠️ Сервер вернул статус %s", resp.Status)
		return
	}

	var item Item
	if err := json.NewDecoder(resp.Body).Decode(&item); err != nil {
		log.Printf("⚠️ Ошибка разбора JSON: %v", err)
		return
	}

	fmt.Printf("📦 Последняя запись:\n%+v\n", item)
}

func main() {
	err := godotenv.Load()
	if err != nil {
		log.Fatal("Ошибка загрузки .env файла:", err)
	}

	token := os.Getenv("API_TOKEN")
	if token == "" {
		log.Fatal("API_TOKEN не задан")
	}

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
			getLastItem(token)
		case "exit":
			fmt.Println("👋 Выход.")
			return
		default:
			fmt.Println("❓ Неизвестная команда. Доступны: last, exit")
		}
	}
}
