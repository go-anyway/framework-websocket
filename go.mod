module github.com/go-anyway/framework-websocket

go 1.25.4

require (
	github.com/go-anyway/framework-config v1.0.0
	github.com/go-anyway/framework-log v1.0.0
	github.com/go-anyway/framework-trace v1.0.0
	github.com/gorilla/websocket v1.4.2
)

replace (
	github.com/go-anyway/framework-config => ../core/config
	github.com/go-anyway/framework-log => ../core/log
	github.com/go-anyway/framework-trace => ../trace
)
