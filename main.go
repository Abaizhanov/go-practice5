package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	_ "github.com/lib/pq"
)

type Book struct {
	ID    int64  `json:"id"`
	Title string `json:"title"`
	Price int64  `json:"price"`
	Genre string `json:"genre"`
}

func main() {
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		log.Fatal("set DATABASE_URL env var (postgres DSN)")
	}
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		log.Fatalf("open db: %v", err)
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		log.Fatalf("ping db: %v", err)
	}

	http.HandleFunc("/books", makeGetBooksHandler(db))
	addr := ":8080"
	log.Printf("listening on %s", addr)
	log.Fatal(http.ListenAndServe(addr, nil))
}

func makeGetBooksHandler(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		books, queryTimeMs, err := getBooks(r.Context(), db, r)
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Query-Time", fmt.Sprintf("%dms", queryTimeMs))

		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		enc := json.NewEncoder(w)
		enc.SetIndent("", "  ")
		if err := enc.Encode(books); err != nil {
			log.Printf("encode response: %v", err)
		}
	}
}

func getBooks(ctx context.Context, db *sql.DB, r *http.Request) ([]Book, int64, error) {
	const (
		defaultLimit = 10
		maxLimit     = 100
	)
	q := r.URL.Query()

	limit := defaultLimit
	if s := strings.TrimSpace(q.Get("limit")); s != "" {
		if v, err := strconv.Atoi(s); err == nil && v > 0 {
			limit = v
		} else {
			return nil, 0, fmt.Errorf("invalid limit")
		}
	}
	if limit > maxLimit {
		limit = maxLimit
	}

	offset := 0
	if s := strings.TrimSpace(q.Get("offset")); s != "" {
		if v, err := strconv.Atoi(s); err == nil && v >= 0 {
			offset = v
		} else {
			return nil, 0, fmt.Errorf("invalid offset")
		}
	}

	genre := strings.TrimSpace(q.Get("genre"))

	sortParam := strings.TrimSpace(q.Get("sort"))
	orderBy := ""
	switch sortParam {
	case "":
	case "price_asc":
		orderBy = "price ASC"
	case "price_desc":
		orderBy = "price DESC"
	default:
		return nil, 0, fmt.Errorf("invalid sort value")
	}

	args := []interface{}{}
	where := []string{}

	if genre != "" {
		args = append(args, genre)
		where = append(where, fmt.Sprintf("genre = $%d", len(args)))
	}

	var sb strings.Builder
	sb.WriteString("SELECT id, title, price, genre FROM books")
	if len(where) > 0 {
		sb.WriteString(" WHERE ")
		sb.WriteString(strings.Join(where, " AND "))
	}

	if orderBy != "" {
		sb.WriteString(" ORDER BY " + orderBy)
	}

	args = append(args, limit)
	sb.WriteString(fmt.Sprintf(" LIMIT $%d", len(args)))

	args = append(args, offset)
	sb.WriteString(fmt.Sprintf(" OFFSET $%d", len(args)))

	query := sb.String()

	start := time.Now()
	rows, err := db.QueryContext(ctx, query, args...)
	elapsed := time.Since(start).Milliseconds()

	log.Printf("SQL: %s | args=%v | took=%dms", query, args, elapsed)

	if err != nil {
		return nil, elapsed, err
	}
	defer rows.Close()

	var books []Book
	for rows.Next() {
		var b Book
		if err := rows.Scan(&b.ID, &b.Title, &b.Price, &b.Genre); err != nil {
			return nil, elapsed, err
		}
		books = append(books, b)
	}
	if err := rows.Err(); err != nil {
		return nil, elapsed, err
	}
	return books, elapsed, nil
}
