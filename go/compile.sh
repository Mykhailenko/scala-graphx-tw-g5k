echo "Building local exec"
go install sparklog
echo "Building for Linux"
GOOS=linux GOARCH=amd64 go build -o bin/sparklog-linux sparklog
