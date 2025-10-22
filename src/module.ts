import { DataSourcePlugin } from '@grafana/data';
import { ArcDataSource } from './datasource';
import { ConfigEditor } from './ConfigEditor';
import { QueryEditor } from './QueryEditor';
import { VariableQueryEditor } from './VariableQueryEditor';
import { ArcQuery, ArcDataSourceOptions } from './types';

export const plugin = new DataSourcePlugin<ArcDataSource, ArcQuery, ArcDataSourceOptions>(ArcDataSource)
  .setConfigEditor(ConfigEditor)
  .setQueryEditor(QueryEditor)
  .setVariableQueryEditor(VariableQueryEditor);
