import React, { ChangeEvent } from 'react';
import { InlineField, Input, SecretInput, Switch } from '@grafana/ui';
import { DataSourcePluginOptionsEditorProps } from '@grafana/data';
import { ArcDataSourceOptions, ArcSecureJsonData } from './types';

interface Props extends DataSourcePluginOptionsEditorProps<ArcDataSourceOptions, ArcSecureJsonData> {}

export function ConfigEditor(props: Props) {
  const { onOptionsChange, options } = props;
  const { jsonData, secureJsonFields, secureJsonData } = options;

  // URL change handler
  const onURLChange = (event: ChangeEvent<HTMLInputElement>) => {
    onOptionsChange({
      ...options,
      jsonData: {
        ...jsonData,
        url: event.target.value,
      },
    });
  };

  // Database change handler
  const onDatabaseChange = (event: ChangeEvent<HTMLInputElement>) => {
    onOptionsChange({
      ...options,
      jsonData: {
        ...jsonData,
        database: event.target.value,
      },
    });
  };

  // Timeout change handler
  const onTimeoutChange = (event: ChangeEvent<HTMLInputElement>) => {
    const timeout = parseInt(event.target.value, 10);
    onOptionsChange({
      ...options,
      jsonData: {
        ...jsonData,
        timeout: isNaN(timeout) ? 30 : timeout,
      },
    });
  };

  // Use Arrow toggle handler
  const onUseArrowChange = (event: ChangeEvent<HTMLInputElement>) => {
    onOptionsChange({
      ...options,
      jsonData: {
        ...jsonData,
        useArrow: event.target.checked,
      },
    });
  };

  const onAllowPrivateIPsChange = (event: ChangeEvent<HTMLInputElement>) => {
    onOptionsChange({
      ...options,
      jsonData: {
        ...jsonData,
        allowPrivateIPs: event.target.checked,
      },
    });
  };

  const onAllowDatabaseOverrideChange = (event: ChangeEvent<HTMLInputElement>) => {
    onOptionsChange({
      ...options,
      jsonData: {
        ...jsonData,
        allowDatabaseOverride: event.target.checked,
      },
    });
  };

  // Max Concurrency change handler
  const onMaxConcurrencyChange = (event: ChangeEvent<HTMLInputElement>) => {
    const val = parseInt(event.target.value, 10);
    onOptionsChange({
      ...options,
      jsonData: {
        ...jsonData,
        maxConcurrency: isNaN(val) || val < 1 ? 4 : val,
      },
    });
  };

  const onMaxResponseMBChange = (event: ChangeEvent<HTMLInputElement>) => {
    const val = parseInt(event.target.value, 10);
    onOptionsChange({
      ...options,
      jsonData: {
        ...jsonData,
        maxResponseMB: isNaN(val) || val < 1 ? 1024 : val,
      },
    });
  };

  // API Key change handler
  const onAPIKeyChange = (event: ChangeEvent<HTMLInputElement>) => {
    onOptionsChange({
      ...options,
      secureJsonData: {
        apiKey: event.target.value,
      },
    });
  };

  // API Key reset handler
  const onResetAPIKey = () => {
    onOptionsChange({
      ...options,
      secureJsonFields: {
        ...secureJsonFields,
        apiKey: false,
      },
      secureJsonData: {
        ...secureJsonData,
        apiKey: '',
      },
    });
  };

  return (
    <div className="gf-form-group">
      <h3 className="page-heading">Arc Connection</h3>

      <InlineField label="URL" labelWidth={20} tooltip="Arc API base URL (e.g., http://localhost:8000)">
        <Input
          width={40}
          value={jsonData.url || ''}
          placeholder="http://localhost:8000"
          onChange={onURLChange}
        />
      </InlineField>

      <InlineField
        label="API Key"
        labelWidth={20}
        tooltip="Arc authentication token with read permissions"
      >
        <SecretInput
          width={40}
          isConfigured={secureJsonFields?.apiKey || false}
          value={secureJsonData?.apiKey || ''}
          placeholder="Your Arc API key"
          onChange={onAPIKeyChange}
          onReset={onResetAPIKey}
        />
      </InlineField>

      <InlineField
        label="Database"
        labelWidth={20}
        tooltip="Default database/schema name (optional, defaults to 'default')"
      >
        <Input
          width={40}
          value={jsonData.database || 'default'}
          placeholder="default"
          onChange={onDatabaseChange}
        />
      </InlineField>

      <h3 className="page-heading">Advanced Settings</h3>

      <InlineField
        label="Timeout"
        labelWidth={20}
        tooltip="Query timeout in seconds"
      >
        <Input
          width={40}
          type="number"
          value={jsonData.timeout || 30}
          placeholder="30"
          onChange={onTimeoutChange}
        />
      </InlineField>

      <InlineField
        label="Max Concurrency"
        labelWidth={20}
        tooltip="Maximum parallel chunks for query splitting. Each Grafana panel can spawn up to this many concurrent Arc requests. Lower values reduce Arc load in multi-user deployments."
      >
        <Input
          width={40}
          type="number"
          value={jsonData.maxConcurrency || 4}
          placeholder="4"
          onChange={onMaxConcurrencyChange}
        />
      </InlineField>

      <InlineField
        label="Max Response MB"
        labelWidth={20}
        tooltip="Maximum response body size in MiB. Default 1024 (1 GiB). Raise for very large analytical queries — Arc emits 'Arrow IPC stream truncated' errors when the cap is hit mid-stream. Lower bounds defense against runaway queries OOMing the plugin."
      >
        <Input
          width={40}
          type="number"
          value={jsonData.maxResponseMB || 1024}
          placeholder="1024"
          onChange={onMaxResponseMBChange}
        />
      </InlineField>

      <InlineField
        label="Use Arrow Protocol"
        labelWidth={20}
        tooltip="Enable Apache Arrow for faster data transfer (recommended)"
      >
        <Switch
          value={jsonData.useArrow ?? true}
          onChange={onUseArrowChange}
        />
      </InlineField>

      <InlineField
        label="Allow Private IPs"
        labelWidth={20}
        tooltip="Permit the Arc URL to resolve to private/RFC1918 addresses (e.g. 10.x, 192.168.x). Off by default — enable when Arc is deployed on an internal corporate network. Loopback (localhost) is always permitted when configured directly."
      >
        <Switch
          value={jsonData.allowPrivateIPs ?? false}
          onChange={onAllowPrivateIPsChange}
        />
      </InlineField>

      <InlineField
        label="Allow Database Override"
        labelWidth={20}
        tooltip="Permit per-query 'database' field to override this datasource's default database. Off by default — without this, a dashboard editor could switch databases on a datasource configured for a single tenant. Enable only if the API key's authorization scope matches dashboard-editor permissions."
      >
        <Switch
          value={jsonData.allowDatabaseOverride ?? false}
          onChange={onAllowDatabaseOverrideChange}
        />
      </InlineField>

      <div className="gf-form-group">
        <div className="gf-form">
          <label className="gf-form-label width-20"></label>
          <div className="gf-form-label">
            <small style={{ color: '#999' }}>
              Arrow protocol provides 3-5x better performance compared to JSON.
              Keep enabled unless debugging.
            </small>
          </div>
        </div>
      </div>
    </div>
  );
}
