build: fmt

	go build -o bin/sqlite-ext main.go

fmt:
	go fmt main.go
