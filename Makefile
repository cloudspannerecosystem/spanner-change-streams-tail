#################################################
GOOS    := $(shell go env GOOS)
GOARCH  := $(shell go env GOARCH)
GOFILES := $(shell ls *.go |grep -v test|grep -v  spanner-change-streams-tail)
GOBUILD := GOOS=$(GOOS) GOARCH=$(GOARCH) go build
MAINFILE := spanner-change-streams-tail.go


#################################################
default:        deps test bin
localbuild:         deps test releaser

deps:
	go get

releaser:
	goreleaser build --snapshot --rm-dist --single-target

test:
	go test -v ./...

bin:
	$(GOBUILD) $(MAINFILE) $(GOFILES) 

