import {
  DataQueryRequest,
  DataQueryResponse,
  DataSourceApi,
  DataSourceInstanceSettings,
  MetricFindValue,
} from '@grafana/data';

import { getBackendSrv } from '@grafana/runtime';

import { ArcQuery, ArcDataSourceOptions } from './types';

export class ArcDataSource extends DataSourceApi<ArcQuery, ArcDataSourceOptions> {
  url?: string;
  database?: string;

  constructor(instanceSettings: DataSourceInstanceSettings<ArcDataSourceOptions>) {
    super(instanceSettings);
    this.url = instanceSettings.jsonData.url;
    this.database = instanceSettings.jsonData.database || 'default';
  }

  /**
   * Query Arc datasource
   * Called by Grafana when a panel needs data
   */
  async query(options: DataQueryRequest<ArcQuery>): Promise<DataQueryResponse> {
    // Use backend plugin to execute queries
    return getBackendSrv().fetch({
      url: '/api/ds/query',
      method: 'POST',
      data: {
        queries: options.targets.map((target) => ({
          ...target,
          datasourceId: this.id,
        })),
        range: options.range,
        from: options.range.from.valueOf().toString(),
        to: options.range.to.valueOf().toString(),
      },
    }).toPromise() as Promise<DataQueryResponse>;
  }

  /**
   * Test datasource connection
   * Called when user clicks "Save & Test"
   */
  async testDatasource() {
    // Backend plugin handles health check
    return getBackendSrv()
      .fetch({
        url: `/api/datasources/${this.id}/health`,
        method: 'GET',
      })
      .toPromise()
      .then((response: any) => {
        if (response.status === 200) {
          return {
            status: 'success',
            message: 'Arc datasource is working',
            title: 'Success',
          };
        }

        return {
          status: 'error',
          message: response.message || 'Unknown error',
          title: 'Error',
        };
      })
      .catch((error: any) => {
        return {
          status: 'error',
          message: error.message || 'Failed to connect to Arc',
          title: 'Error',
        };
      });
  }

  /**
   * Get metric find values for template variables
   */
  async metricFindQuery(query: string): Promise<MetricFindValue[]> {
    // Use backend to execute query
    const response = await getBackendSrv()
      .fetch({
        url: '/api/ds/query',
        method: 'POST',
        data: {
          queries: [
            {
              refId: 'metricFindQuery',
              sql: query,
              datasourceId: this.id,
            },
          ],
        },
      })
      .toPromise();

    if (!response.data || !response.data.results) {
      return [];
    }

    const result = response.data.results['metricFindQuery'];
    if (!result || !result.frames || result.frames.length === 0) {
      return [];
    }

    const frame = result.frames[0];
    if (!frame.data || !frame.data.values || frame.data.values.length === 0) {
      return [];
    }

    // Convert first column to metric find values
    const values = frame.data.values[0];
    return values.map((value: any) => ({
      text: String(value),
      value: value,
    }));
  }
}
