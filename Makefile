all: clean coboo

coffo:
	go get
	go build -o coffo main.go

clean:
	[ -f ./coffo ]&&rm ./coffo|| :

