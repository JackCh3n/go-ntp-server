APP_NAME=ntp-server
BUILD_DIR=build

all: linux windows mac mac-arm

linux:
	mkdir -p $(BUILD_DIR)
	GOOS=linux GOARCH=amd64 go build -o $(BUILD_DIR)/$(APP_NAME)-linux main.go

windows:
	mkdir -p $(BUILD_DIR)
	GOOS=windows GOARCH=amd64 go build -o $(BUILD_DIR)/$(APP_NAME)-windows.exe main.go

mac:
	mkdir -p $(BUILD_DIR)
	GOOS=darwin GOARCH=amd64 go build -o $(BUILD_DIR)/$(APP_NAME)-mac main.go

mac-arm:
	mkdir -p $(BUILD_DIR)
	GOOS=darwin GOARCH=arm64 go build -o $(BUILD_DIR)/$(APP_NAME)-mac-arm main.go

clean:
	rm -rf $(BUILD_DIR)

.PHONY: all linux windows mac mac-arm clean