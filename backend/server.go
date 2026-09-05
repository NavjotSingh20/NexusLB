package main

import (
	"fmt"
	"net/http"
)

func handler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintln(w, "response from backend server 1")
}

func main() {
	http.HandleFunc("/", handler)

	fmt.Println("backend 1 running on http://localhost:8001")

	err := http.ListenAndServe(":8001", nil)
	if err != nil {
		fmt.Println("server error:", err)
	}
}
