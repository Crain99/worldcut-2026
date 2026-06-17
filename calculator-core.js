(function(root, factory) {
  const api = factory();
  if (typeof module === 'object' && module.exports) module.exports = api;
  root.CalculatorCore = api;
})(typeof globalThis !== 'undefined' ? globalThis : this, function() {
  const PICK_LABELS = ['主胜', '平局', '客胜'];

  function normalizePassType(value) {
    const text = String(value || '').trim();
    if (!text || text === '单关' || text.toLowerCase() === 'single') return 'single';
    const matched = text.match(/(\d+)\s*(?:串|x)\s*1/i);
    return matched ? `${matched[1]}x1` : 'single';
  }

  function denormalizePassType(value) {
    const normalized = normalizePassType(value);
    return normalized === 'single' ? '单关' : normalized.replace('x', '串');
  }

  function passSize(passType) {
    const normalized = normalizePassType(passType);
    return normalized === 'single' ? 1 : Number(normalized.split('x')[0]);
  }

  function combinations(items, size) {
    if (size <= 0) return [[]];
    if (size > items.length) return [];
    const result = [];
    const walk = (start, combo) => {
      if (combo.length === size) {
        result.push(combo.slice());
        return;
      }
      for (let i = start; i <= items.length - (size - combo.length); i++) {
        combo.push(items[i]);
        walk(i + 1, combo);
        combo.pop();
      }
    };
    walk(0, []);
    return result;
  }

  function limitedPayout(productOdds, passSizeValue, times) {
    let base = Number(productOdds || 0) * 2;
    if ([2, 3].includes(passSizeValue) && base > 200000) base = 200000;
    if ([4, 5].includes(passSizeValue) && base > 500000) base = 500000;
    if (passSizeValue >= 6 && passSizeValue <= 8 && base > 1000000) base = 1000000;
    return Number((base * Math.max(1, Number(times || 1))).toFixed(2));
  }

  function cartesianOdds(legs) {
    const result = [];
    const walk = (idx, current) => {
      if (idx >= legs.length) {
        result.push(current.slice());
        return;
      }
      for (const odd of legs[idx].odds || []) {
        current.push({leg: legs[idx], odd});
        walk(idx + 1, current);
        current.pop();
      }
    };
    walk(0, []);
    return result;
  }

  function cleanLegs(legs) {
    return (Array.isArray(legs) ? legs : [])
      .map(leg => ({
        ...leg,
        odds: (leg.odds || [])
          .map(odd => ({...odd, odds: Number(odd.odds)}))
          .filter(odd => Number.isFinite(odd.odds) && odd.odds > 1),
        supportsSingle: leg.supportsSingle !== false,
        supportsAllUp: leg.supportsAllUp !== false
      }))
      .filter(leg => leg.odds.length);
  }

  function calculateBonus(input) {
    const legs = cleanLegs(input?.legs || []);
    const normalizedPassType = normalizePassType(input?.passType || 'single');
    const size = passSize(normalizedPassType);
    const times = Math.max(1, Math.floor(Number(input?.times || 1)));
    const eligible = legs.filter(leg => normalizedPassType === 'single' ? leg.supportsSingle : leg.supportsAllUp);
    const combos = combinations(eligible, size);
    let betCount = 0;
    let minPayout = 0;
    let maxPayout = 0;
    for (const combo of combos) {
      let count = 1;
      let minProduct = 1;
      let maxProduct = 1;
      for (const leg of combo) {
        const odds = leg.odds.map(item => item.odds);
        count *= odds.length;
        minProduct *= Math.min(...odds);
        maxProduct *= Math.max(...odds);
      }
      betCount += count;
      minPayout += limitedPayout(minProduct, size, times);
      maxPayout += limitedPayout(maxProduct, size, times);
    }
    const stake = betCount * 2 * times;
    const profit = maxPayout - stake;
    const breakEven = maxPayout > 0 ? (stake / maxPayout) * 100 : 0;
    return {
      times,
      passType: normalizedPassType,
      passSize: size,
      candidateCount: Number(input?.candidateCount || 0),
      selectedCount: legs.length,
      eligibleCount: eligible.length,
      betCount,
      stake,
      minPayout: Number(minPayout.toFixed(2)),
      maxPayout: Number(maxPayout.toFixed(2)),
      profit: Number(profit.toFixed(2)),
      breakEven
    };
  }

  function ticketLeg(entry, formatMarket) {
    const match = entry.leg.match || {};
    const odd = entry.odd || {};
    return {
      matchNo: match.matchNo || entry.leg.matchNo || '',
      match: entry.leg.matchLabel || `${match.home || ''} vs ${match.away || ''}`.trim(),
      market: entry.leg.marketLabel || (formatMarket ? formatMarket(match) : ''),
      pick: odd.label || PICK_LABELS[odd.idx] || '',
      odds: Number(odd.odds || 0)
    };
  }

  function buildTicket(entryList, calc, index, options={}) {
    const legs = entryList.map(entry => ticketLeg(entry, options.formatMarket));
    const combinedOdds = legs.reduce((acc, leg) => acc * Number(leg.odds || 1), 1);
    const payout = limitedPayout(combinedOdds, calc.passSize, calc.times);
    const stake = 2 * calc.times;
    const serialBase = options.serialBase || 'SIM';
    const issuedAt = options.issuedAt || new Date().toLocaleString('zh-CN', {hour12: false});
    return {
      index,
      serial: `${serialBase}-${String(index).padStart(3, '0')}`,
      issuedAt,
      passType: denormalizePassType(calc.passType),
      times: calc.times,
      stake,
      combinedOdds: Number(combinedOdds.toFixed(2)),
      payout,
      profit: Number((payout - stake).toFixed(2)),
      legs
    };
  }

  function buildTickets(input) {
    const limit = Math.max(1, Number(input?.limit || 120));
    const calc = input?.calc || calculateBonus(input);
    const legs = cleanLegs(input?.legs || []);
    const tickets = [];
    const pushTicket = entries => {
      if (tickets.length >= limit) return;
      tickets.push(buildTicket(entries, calc, tickets.length + 1, input));
    };
    if (!legs.length || !calc.betCount) {
      return {calc, legs, tickets, omitted: 0};
    }
    if (calc.passType === 'single') {
      for (const leg of legs.filter(item => item.supportsSingle)) {
        for (const odd of leg.odds) pushTicket([{leg, odd}]);
      }
    } else {
      const eligible = legs.filter(leg => leg.supportsAllUp);
      const combos = combinations(eligible, calc.passSize);
      for (const combo of combos) {
        for (const pickCombo of cartesianOdds(combo)) pushTicket(pickCombo);
      }
    }
    return {calc, legs, tickets, omitted: Math.max(0, calc.betCount - tickets.length)};
  }

  return {
    PICK_LABELS,
    normalizePassType,
    denormalizePassType,
    passSize,
    combinations,
    limitedPayout,
    cartesianOdds,
    calculateBonus,
    buildTicket,
    buildTickets
  };
});
