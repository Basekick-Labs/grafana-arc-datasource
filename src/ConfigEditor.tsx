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
        label="Use Arrow Protocol"
        labelWidth={20}
        tooltip="Enable Apache Arrow for faster data transfer (recommended)"
      >
        <Switch
          value={jsonData.useArrow ?? true}
          onChange={onUseArrowChange}
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
