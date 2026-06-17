const assert = require('node:assert/strict');
const SimulatorCore = require('../simulator-core.js');

{
  const account = {
    cash: 920,
    bets: [
      {key: 'a', type: 'single', stake: 100, status: '待赛'},
      {key: 'b', type: 'parlay', stake: 50, status: '待赛'},
      {key: 'c', type: 'single', stake: 80, status: '已中', settledProfit: 120},
      {key: 'd', type: 'single', stake: 40, status: '未中', settledProfit: -40},
      {key: 'e', type: 'single', stake: 30, status: '已取消'}
    ]
  };
  const stats = SimulatorCore.calculateStats(account);
  assert.equal(stats.total_bets, 5);
  assert.equal(stats.pending_bets, 2);
  assert.equal(stats.settled_bets, 2);
  assert.equal(stats.single_bets, 4);
  assert.equal(stats.parlay_bets, 1);
  assert.equal(stats.pending_stake, 150);
  assert.equal(stats.realized_profit, 80);
  assert.equal(stats.win_rate, '50.0');
  assert.equal(stats.roi, '66.7');
}

{
  const account = {
    bets: [
      {key: 'old', status: '待赛', calculatorSessionId: 's1'},
      {key: 'new-a', status: '待赛', calculatorSessionId: 's2'},
      {key: 'new-b', status: '待赛', calculatorSessionId: 's2'},
      {key: 'done', status: '已中', calculatorSessionId: 's2'}
    ]
  };
  assert.deepEqual(SimulatorCore.chooseBetsForCalculator(account).map(bet => bet.key), ['new-a', 'new-b']);
  assert.deepEqual(SimulatorCore.chooseBetsForCalculator(account, 'old').map(bet => bet.key), ['old']);
}

{
  const account = {
    bets: [
      {key: 'single', id: 17, status: '待赛', home: '法国', away: '塞内加尔'},
      {
        key: 'parlay',
        type: 'parlay',
        status: '待赛',
        legs: [
          {id: 'tc-1', home: '法国', away: '塞内加尔'},
          {id: 'tc-2', home: '奥地利', away: '约旦'}
        ]
      },
      {key: 'settled', id: 99, status: '已中'}
    ]
  };
  const resolver = item => `${item.home}-${item.away}`;
  const mapped = SimulatorCore.mapActiveBets(account, resolver);
  assert.equal(mapped.active.get('17').key, 'single');
  assert.equal(mapped.active.get('法国-塞内加尔').key, 'single');
  assert.equal(mapped.parlays.get('tc-1').key, 'parlay');
  assert.equal(mapped.parlays.get('奥地利-约旦').key, 'parlay');
  assert.equal(mapped.active.has('99'), false);
}

console.log('simulator-core tests passed');
