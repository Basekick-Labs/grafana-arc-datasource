/**
 * Edit-time rules for dashboard variables in builder queries (issue #20,
 * plan v3 §4.4). The generator writes variables bare and builderFormat
 * quotes their values; these rules keep a variable out of places where its
 * value cannot work.
 */
import { BuilderQuery } from '../types';
import { FieldError, variableName } from './generate';

/** The parts of a Grafana variable model the rules read. */
export interface VariableInfo {
  name: string;
  multi?: boolean;
  includeAll?: boolean;
  allValue?: string | null;
}

const MULTI_OK = new Set(['=', '!=']);

/**
 * Errors for variables used where they cannot work. `inAlerting`: alert
 * rules are evaluated without dashboard variables, so none may be used.
 */
export function variableErrors(
  b: BuilderQuery,
  database: string | undefined,
  variables: VariableInfo[],
  inAlerting: boolean
): FieldError[] {
  const byName = new Map(variables.map((v) => [v.name, v]));
  const errors: FieldError[] = [];

  const check = (field: string, ref: string | undefined, singleOnly: boolean, op?: string) => {
    const name = variableName(ref);
    if (name === undefined) {
      return;
    }
    if (inAlerting) {
      errors.push({
        field,
        message: `Alert rules are evaluated without dashboard variables; replace $${name} with a value`,
      });
      return;
    }
    const v = byName.get(name);
    if (!v) {
      if (!name.startsWith('__')) {
        errors.push({ field, message: `No variable named $${name} on this dashboard` });
      }
      return;
    }
    if (v.includeAll && v.allValue) {
      errors.push({
        field,
        message: `$${name} has a custom "All" value, which the builder cannot use in a filter. Remove the custom All value, or use Code mode`,
      });
      return;
    }
    if ((v.multi || v.includeAll) && (singleOnly || (op !== undefined && !MULTI_OK.has(op)))) {
      const where = singleOnly ? 'as a database or measurement' : `with ${op}`;
      errors.push({
        field,
        message: `$${name} can hold several values, which cannot be used ${where}; use = or != , or a single-value variable`,
      });
    }
  };

  check('database', database, true);
  check('measurement', b.measurement, true);
  b.where.forEach((f, i) => check(`where.${i}.value`, f.value, false, f.op));
  return errors;
}
