const assert = require('node:assert/strict');
const CalculatorCore = require('../calculator-core.js');

function leg(name, odds, options={}) {
  return {
    key: name,
    fixtureKey: name,
    matchLabel: name,
    marketLabel: options.marketLabel || '胜平负',
    odds,
    supportsSingle: options.supportsSingle,
    supportsAllUp: options.supportsAllUp
  };
}

{
  const legs = [
    leg('法国 vs 塞内加尔', [{label: '主胜', odds: 1.33}]),
    leg('奥地利 vs 约旦', [{label: '主胜', odds: 1.25}])
  ];
  const calc = CalculatorCore.calculateBonus({legs, passType: '2x1', times: 50, candidateCount: 2});
  assert.equal(calc.passType, '2x1');
  assert.equal(calc.passSize, 2);
  assert.equal(calc.betCount, 1);
  assert.equal(calc.stake, 100);
  assert.equal(calc.maxPayout, 166.25);
  assert.equal(calc.profit, 66.25);
}

{
  const legs = [
    leg('法国 vs 塞内加尔', [{label: '主胜', odds: 1.33}]),
    leg('伊拉克 vs 挪威', [{label: '客胜', odds: 2.29}], {marketLabel: '让球胜平负(+2)'}),
    leg('阿根廷 vs 阿尔及利亚', [{label: '主胜', odds: 1.26}]),
    leg('奥地利 vs 约旦', [{label: '主胜', odds: 1.25}])
  ];
  const calc = CalculatorCore.calculateBonus({legs, passType: '4串1', times: 1, candidateCount: 4});
  assert.equal(calc.passType, '4x1');
  assert.equal(calc.betCount, 1);
  assert.equal(calc.stake, 2);
  assert.equal(calc.maxPayout, 9.59);
  const {tickets} = CalculatorCore.buildTickets({legs, calc, serialBase: 'SIM-TEST', issuedAt: '2026-06-17 10:00:00'});
  assert.equal(tickets.length, 1);
  assert.equal(tickets[0].serial, 'SIM-TEST-001');
  assert.equal(tickets[0].passType, '4串1');
  assert.equal(tickets[0].legs[1].market, '让球胜平负(+2)');
  assert.equal(tickets[0].legs[1].pick, '客胜');
  assert.equal(tickets[0].payout, 9.59);
}

{
  const legs = [
    leg('单关支持', [{label: '主胜', odds: 1.5}], {supportsSingle: true}),
    leg('不支持单关', [{label: '客胜', odds: 2.2}], {supportsSingle: false})
  ];
  const calc = CalculatorCore.calculateBonus({legs, passType: 'single', times: 10, candidateCount: 2});
  assert.equal(calc.betCount, 1);
  assert.equal(calc.stake, 20);
  assert.equal(calc.maxPayout, 30);
}

{
  const legs = [
    leg('多选A', [{label: '主胜', odds: 1.8}, {label: '平局', odds: 3.1}]),
    leg('多选B', [{label: '客胜', odds: 2.0}])
  ];
  const calc = CalculatorCore.calculateBonus({legs, passType: '2串1', times: 1, candidateCount: 2});
  assert.equal(calc.betCount, 2);
  assert.equal(calc.stake, 4);
  assert.equal(calc.minPayout, 7.2);
  assert.equal(calc.maxPayout, 12.4);
}

console.log('calculator-core tests passed');
