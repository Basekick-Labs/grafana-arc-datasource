package main

import (
	"os"

	"github.com/basekick-labs/grafana-arc-datasource/pkg/plugin"
	"github.com/grafana/grafana-plugin-sdk-go/backend/datasource"
	"github.com/grafana/grafana-plugin-sdk-go/backend/log"
)

func main() {
	ds := plugin.NewArcDatasource()
	
	// Serve plugin
	if err := datasource.Serve(datasource.ServeOpts{
		QueryDataHandler:   ds,
		CheckHealthHandler: ds,
	}); err != nil {
		log.DefaultLogger.Error(err.Error())
		os.Exit(1)
	}
}
