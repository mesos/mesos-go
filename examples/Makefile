BIN := scheduler executor zkdetect
.PHONY:
.PHONY: all $(BIN)

all: $(BIN)

$(BIN):
	mkdir -p _output
	godep go build -o _output/$@ ./$@
