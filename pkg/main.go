package main

import (
	"os"

	// Embed the IANA timezone database in the binary.
	//
	// $__timeGroup resolves the dashboard's timezone with time.LoadLocation,
	// which otherwise reads the HOST's zoneinfo. Grafana's Alpine and Ubuntu
	// images ship tzdata, but the distroless variant and Windows hosts do not,
	// and the failure is silent: every zone degrades to UTC and non-UTC
	// dashboards bucket on the wrong boundaries with no error. ~450 KB is a
	// cheap price for that not happening.
	_ "time/tzdata"

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
