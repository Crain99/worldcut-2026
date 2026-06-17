(function(root, factory) {
  const api = factory();
  if (typeof module === 'object' && module.exports) module.exports = api;
  root.SimulatorCore = api;
})(typeof globalThis !== 'undefined' ? globalThis : this, function() {
  const SETTLED_STATUSES = new Set(['已中', '未中']);
  const INACTIVE_STATUSES = new Set(['已中', '未中', '已取消']);

  function betsOf(account) {
    return Array.isArray(account?.bets) ? account.bets : [];
  }

  function isSettledBet(bet) {
    return SETTLED_STATUSES.has(bet?.status);
  }

  function isPendingBet(bet) {
    return !INACTIVE_STATUSES.has(bet?.status);
  }

  function pendingBets(account) {
    return betsOf(account).filter(isPendingBet);
  }

  function calculateStats(account) {
    const bets = betsOf(account);
    const settled = bets.filter(isSettledBet);
    const pending = bets.filter(isPendingBet);
    const won = settled.filter(bet => bet.status === '已中');
    const settledStake = settled.reduce((sum, bet) => sum + Number(bet.stake || 0), 0);
    const pendingStake = pending.reduce((sum, bet) => sum + Number(bet.stake || 0), 0);
    const realized = settled.reduce((sum, bet) => sum + Number(bet.settledProfit || 0), 0);
    return {
      total_bets: bets.length,
      pending_bets: pending.length,
      settled_bets: settled.length,
      single_bets: bets.filter(bet => bet.type !== 'parlay').length,
      parlay_bets: bets.filter(bet => bet.type === 'parlay').length,
      pending_stake: Math.round(pendingStake),
      realized_profit: Math.round(realized),
      win_rate: settled.length ? ((won.length / settled.length) * 100).toFixed(1) : 0,
      roi: settledStake ? ((realized / settledStake) * 100).toFixed(1) : 0
    };
  }

  function betIdentity(bet) {
    return String(bet?.key || bet?.id || '');
  }

  function chooseBetsForCalculator(account, targetKey='') {
    const pending = pendingBets(account);
    if (!pending.length) return [];
    const requested = String(targetKey || '');
    const anchor = requested
      ? pending.find(bet => betIdentity(bet) === requested)
      : pending.at(-1);
    if (!anchor) return [];
    if (anchor.calculatorSessionId) {
      return pending.filter(bet => bet.calculatorSessionId === anchor.calculatorSessionId);
    }
    return [anchor];
  }

  function idsForBetItem(item, resolveScheduleId) {
    const ids = new Set();
    if (item?.id !== undefined && item?.id !== null) ids.add(String(item.id));
    if (typeof resolveScheduleId === 'function') {
      const resolved = resolveScheduleId(item);
      if (resolved !== undefined && resolved !== null && resolved !== '') ids.add(String(resolved));
    }
    return [...ids];
  }

  function mapActiveBets(account, resolveScheduleId) {
    const active = new Map();
    const parlays = new Map();
    for (const bet of betsOf(account)) {
      if (!isPendingBet(bet)) continue;
      if (bet.type === 'parlay') {
        const ids = Array.isArray(bet.legs) && bet.legs.length
          ? bet.legs.flatMap(leg => idsForBetItem(leg, resolveScheduleId))
          : [];
        for (const id of ids) parlays.set(id, bet);
      } else {
        for (const id of idsForBetItem(bet, resolveScheduleId)) active.set(id, bet);
      }
    }
    return {active, parlays};
  }

  return {
    isSettledBet,
    isPendingBet,
    pendingBets,
    calculateStats,
    betIdentity,
    chooseBetsForCalculator,
    idsForBetItem,
    mapActiveBets
  };
});
