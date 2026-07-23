import test from 'node:test';
import assert from 'node:assert/strict';
import { isValidNodeConnection } from './NodePropagation.js';

test('utility nodes accept calculator and combine-column connections', () => {
  assert.equal(isValidNodeConnection('input_dataset', 'calc'), true);
  assert.equal(isValidNodeConnection('calc', 'output_dataset'), true);
  assert.equal(isValidNodeConnection('adv_calculator', 'combine_cols'), true);
  assert.equal(isValidNodeConnection('combine_cols', 'output_dataset'), true);
});
