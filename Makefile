PACKAGE_DIRS := $(shell find . -mindepth 2 -type f -name 'go.mod' -exec dirname {} \; | sort)

test: testdeps
	go test ./...
	go test ./... -short -race
	go test ./... -run=NONE -bench=. -benchmem
	env GOOS=linux GOARCH=386 go test ./...
	go vet

testdeps: testdata/skytable/

bench: testdeps
	go test ./... -test.run=NONE -test.bench=. -test.benchmem

.PHONY: all test testdeps bench

testdata/skytable/:
	mkdir -p $@
	wget -qO- https://github.com/skytable/skytable/releases/download/v0.7.5/sky-bundle-v0.7.5-x86_64-linux-gnu.zip -O $@/skytable.zip
	unzip -q $@/skytable.zip -d $@
	rm $@/skytable.zip

fmt:
	gofmt -w -s ./
	goimports -w  -local github.com/satvik007/skytable-go ./

go_mod_tidy:
	set -e; for dir in $(PACKAGE_DIRS); do \
	  echo "go mod tidy in $${dir}"; \
	  (cd "$${dir}" && \
	    go get -u ./... && \
	    go mod tidy -compat=1.17); \
	done
