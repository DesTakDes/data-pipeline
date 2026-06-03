import { genId } from './utils';

// src/utils.js
let _idSeq = 0;
export const genId = () => 
  `n${Date.now()}_${++_idSeq}_${Math.random().toString(36).slice(2, 6)}`;
