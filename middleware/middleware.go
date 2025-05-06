package middleware

import (
	"net/http"
	"strings"
)

func AuthMiddleware(expectedToken string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Пробуем получить токен из заголовка
			authHeader := r.Header.Get("Authorization")
			if strings.HasPrefix(authHeader, "Bearer ") {
				token := strings.TrimPrefix(authHeader, "Bearer ")
				if token == expectedToken {
					next.ServeHTTP(w, r)
					return
				}
			}

			http.Error(w, "Unathorized", http.StatusUnauthorized)
		})
	}
}
