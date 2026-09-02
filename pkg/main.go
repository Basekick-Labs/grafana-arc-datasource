package main

import (
	"os"

	"github.com/basekick-labs/grafana-arc-datasource/pkg/plugin"
	"github.com/grafana/grafana-plugin-sdk-go/backend/datasource"
	"github.com/grafana/grafana-plugin-sdk-go/backend/log"
)

func main() {
	ds := plugin.NewArcDatasource()

	// Serve rather than Manage: ArcDatasource carries its own InstanceManager
	// (see plugin.NewArcDatasource) so instance caching and disposal are
	// already handled; migrating to datasource.Manage means inverting that
	// design and is tracked as a standalone refactor.
	//nolint:staticcheck
	if err := datasource.Serve(datasource.ServeOpts{
		QueryDataHandler:   ds,
		CheckHealthHandler: ds,
	}); err != nil {
		log.DefaultLogger.Error(err.Error())
		os.Exit(1)
	}
}
