.PHONY: build clean deploy

build:
	@echo "Building capacity-poller Lambda..."
	@# Build the PACKAGE (`.`), not `main.go` (#172) — same reason as the
	@# GoReleaser hook in .goreleaser.yaml. Naming files compiles only those
	@# files, so a second source file either breaks the build or, worse, is
	@# silently omitted from the binary.
	GOOS=linux GOARCH=arm64 go build -tags lambda.norpc -o bootstrap .
	zip function.zip bootstrap
	@echo "Build complete: function.zip"

clean:
	rm -f bootstrap function.zip

deploy: build
	@echo "Deploying to Lambda..."
	aws lambda update-function-code \
		--function-name lagotto-capacity-poller \
		--zip-file fileb://function.zip
	@echo "Deploy complete"
