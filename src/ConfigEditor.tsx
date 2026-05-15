import React, { ChangeEvent } from 'react';
import { InlineField, Input, SecretInput, Switch, useStyles2 } from '@grafana/ui';
import { DataSourcePluginOptionsEditorProps, GrafanaTheme2 } from '@grafana/data';
import { css } from '@emotion/css';
import { ArcDataSourceOptions, ArcSecureJsonData } from './types';

interface Props extends DataSourcePluginOptionsEditorProps<ArcDataSourceOptions, ArcSecureJsonData> {}

// Label width sized for the longest label ("Allow Database Override").
// Previously labelWidth={20} chopped that string, the label wrapped onto two
// lines, and the toggle slid out of horizontal alignment with the rows above.
const LABEL_WIDTH = 26;
const INPUT_WIDTH = 40;

export function ConfigEditor(props: Props) {
  const { onOptionsChange, options } = props;
  const { jsonData, secureJsonFields, secureJsonData } = options;
  const styles = useStyles2(getStyles);

  const onURLChange = (event: ChangeEvent<HTMLInputElement>) => {
    onOptionsChange({ ...options, jsonData: { ...jsonData, url: event.target.value } });
  };

  const onDatabaseChange = (event: ChangeEvent<HTMLInputElement>) => {
    onOptionsChange({ ...options, jsonData: { ...jsonData, database: event.target.value } });
  };

  // Numeric handlers split into onChange / onBlur so the user can clear
  // an input and type a new value without `parseInt('') → NaN` snapping
  // the field back to the default mid-keystroke.
  //
  // onChange: store whatever parses cleanly OR `undefined` if the input
  //   is empty/invalid. The Input displays the user's raw text via the
  //   placeholder fallback so typing flows naturally.
  // onBlur: clamp to the field's minimum + apply the default if the
  //   user left the input empty or below 1. Persists the final value.
  const handleNumericChange =
    (key: 'timeout' | 'maxConcurrency' | 'maxResponseMB') =>
    (event: ChangeEvent<HTMLInputElement>) => {
      const parsed = parseInt(event.target.value, 10);
      const next = isNaN(parsed) ? undefined : parsed;
      onOptionsChange({ ...options, jsonData: { ...jsonData, [key]: next } });
    };

  const handleNumericBlur =
    (key: 'timeout' | 'maxConcurrency' | 'maxResponseMB', fallback: number) =>
    () => {
      const current = jsonData[key];
      if (current === undefined || current === null || current < 1) {
        onOptionsChange({ ...options, jsonData: { ...jsonData, [key]: fallback } });
      }
    };

  const onTimeoutChange = handleNumericChange('timeout');
  const onTimeoutBlur = handleNumericBlur('timeout', 30);
  const onMaxConcurrencyChange = handleNumericChange('maxConcurrency');
  const onMaxConcurrencyBlur = handleNumericBlur('maxConcurrency', 4);
  const onMaxResponseMBChange = handleNumericChange('maxResponseMB');
  const onMaxResponseMBBlur = handleNumericBlur('maxResponseMB', 1024);

  const onUseArrowChange = (event: ChangeEvent<HTMLInputElement>) => {
    onOptionsChange({ ...options, jsonData: { ...jsonData, useArrow: event.target.checked } });
  };

  const onAllowPrivateIPsChange = (event: ChangeEvent<HTMLInputElement>) => {
    onOptionsChange({ ...options, jsonData: { ...jsonData, allowPrivateIPs: event.target.checked } });
  };

  const onAllowDatabaseOverrideChange = (event: ChangeEvent<HTMLInputElement>) => {
    onOptionsChange({ ...options, jsonData: { ...jsonData, allowDatabaseOverride: event.target.checked } });
  };

  const onAPIKeyChange = (event: ChangeEvent<HTMLInputElement>) => {
    // Spread existing secureJsonData rather than overwrite. Currently
    // `apiKey` is the only secure field, but if another lands later the
    // overwrite form would silently drop it on every keystroke in the API
    // key input. `onResetAPIKey` below already uses this pattern.
    onOptionsChange({ ...options, secureJsonData: { ...secureJsonData, apiKey: event.target.value } });
  };

  const onResetAPIKey = () => {
    onOptionsChange({
      ...options,
      secureJsonFields: { ...secureJsonFields, apiKey: false },
      secureJsonData: { ...secureJsonData, apiKey: '' },
    });
  };

  return (
    <div className="gf-form-group">
      <h3 className="page-heading">Arc Connection</h3>

      <InlineField label="URL" labelWidth={LABEL_WIDTH} tooltip="Arc API base URL (e.g., http://localhost:8000)">
        <Input
          width={INPUT_WIDTH}
          value={jsonData.url || ''}
          placeholder="http://localhost:8000"
          onChange={onURLChange}
        />
      </InlineField>

      <InlineField label="API Key" labelWidth={LABEL_WIDTH} tooltip="Arc authentication token with read permissions">
        <SecretInput
          width={INPUT_WIDTH}
          isConfigured={secureJsonFields?.apiKey || false}
          value={secureJsonData?.apiKey || ''}
          placeholder="Your Arc API key"
          onChange={onAPIKeyChange}
          onReset={onResetAPIKey}
        />
      </InlineField>

      <InlineField
        label="Database"
        labelWidth={LABEL_WIDTH}
        tooltip="Default database/schema name (optional, defaults to 'default')"
      >
        <Input
          width={INPUT_WIDTH}
          value={jsonData.database || 'default'}
          placeholder="default"
          onChange={onDatabaseChange}
        />
      </InlineField>

      <h3 className="page-heading">Advanced Settings</h3>

      <InlineField label="Timeout" labelWidth={LABEL_WIDTH} tooltip="Query timeout in seconds">
        <Input
          width={INPUT_WIDTH}
          type="number"
          value={jsonData.timeout ?? ''}
          placeholder="30"
          onChange={onTimeoutChange}
          onBlur={onTimeoutBlur}
        />
      </InlineField>

      <InlineField
        label="Max Concurrency"
        labelWidth={LABEL_WIDTH}
        tooltip="Maximum parallel chunks for query splitting. Each Grafana panel can spawn up to this many concurrent Arc requests. Lower values reduce Arc load in multi-user deployments."
      >
        <Input
          width={INPUT_WIDTH}
          type="number"
          value={jsonData.maxConcurrency ?? ''}
          placeholder="4"
          onChange={onMaxConcurrencyChange}
          onBlur={onMaxConcurrencyBlur}
        />
      </InlineField>

      <InlineField
        label="Max Response MB"
        labelWidth={LABEL_WIDTH}
        tooltip="Maximum response body size in MiB. Default 1024 (1 GiB). Raise for very large analytical queries — Arc emits 'Arrow IPC stream truncated' errors when the cap is hit mid-stream. Lower bounds defense against runaway queries OOMing the plugin."
      >
        <Input
          width={INPUT_WIDTH}
          type="number"
          value={jsonData.maxResponseMB ?? ''}
          placeholder="1024"
          onChange={onMaxResponseMBChange}
          onBlur={onMaxResponseMBBlur}
        />
      </InlineField>

      <InlineField
        label="Use Arrow Protocol"
        labelWidth={LABEL_WIDTH}
        tooltip="Apache Arrow is a columnar binary format. 3–5x faster than JSON on the wire and on the plugin's decode hot path. Keep enabled unless debugging."
      >
        <div className={styles.switchCell}>
          <Switch value={jsonData.useArrow ?? true} onChange={onUseArrowChange} />
        </div>
      </InlineField>

      <InlineField
        label="Allow Private IPs"
        labelWidth={LABEL_WIDTH}
        tooltip="Permit the Arc URL to resolve to private/RFC1918 addresses (e.g. 10.x, 192.168.x). Off by default — enable when Arc is deployed on an internal corporate network. Loopback (localhost) is always permitted when configured directly."
      >
        <div className={styles.switchCell}>
          <Switch value={jsonData.allowPrivateIPs ?? false} onChange={onAllowPrivateIPsChange} />
        </div>
      </InlineField>

      <InlineField
        label="Allow Database Override"
        labelWidth={LABEL_WIDTH}
        tooltip="Permit per-query 'database' field to override this datasource's default database. Off by default — without this, a dashboard editor could switch databases on a datasource configured for a single tenant. Enable only if the API key's authorization scope matches dashboard-editor permissions."
      >
        <div className={styles.switchCell}>
          <Switch value={jsonData.allowDatabaseOverride ?? false} onChange={onAllowDatabaseOverrideChange} />
        </div>
      </InlineField>
    </div>
  );
}

// `InlineField` hardcodes `align-items: flex-start`, leaving the Switch
// sitting at the top edge of the row instead of vertically centered against
// the label. The label is `theme.spacing(4)` tall (line-height padding); the
// Switch is half that. The wrapper centers the Switch within the cell.
// Matches the useStyles2 pattern in QueryEditor / VariableQueryEditor and
// adapts to theme changes (light/dark mode) automatically.
const getStyles = (theme: GrafanaTheme2) => ({
  switchCell: css({
    display: 'flex',
    alignItems: 'center',
    height: theme.spacing(4),
  }),
});
